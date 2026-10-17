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
//!
//! The recording functions are `pub(crate)` on purpose, and every caller lives
//! in this crate. A function nothing calls is then dead code, which clippy
//! refuses, so a metric cannot be declared here and quietly never recorded —
//! which is how sixteen of them sat in this file, unrecorded, while the
//! documentation described them. A metric is added together with the code that
//! records it (plan `EXTENDED-METRICS.md`).
//!
//! [`METRIC_CATALOG`] lists every metric S4 exports outside the erasure-coding
//! family. The node describes the metrics from it and registers every series
//! of a cluster node at zero from it, so each exists from the start; scenario
//! 55 checks `/metrics` against the same list.

use std::collections::HashMap;
use std::future::Future;
use std::sync::RwLock;
use std::time::Instant;

use crate::error::ClusterError;
use crate::gossip::NodeState;
use crate::identity::{NodeId, NodeStatus};

/// Bucket bounds, in seconds, of every histogram S4 exports.
///
/// One set for all of them and for every node, so that buckets of different
/// nodes add up into a percentile for the cluster.
pub const DURATION_BUCKETS_SECONDS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

/// The Prometheus type of a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// Only ever grows, from zero at the node's start.
    Counter,
    /// A value that goes up and down.
    Gauge,
    /// Observations counted into [`DURATION_BUCKETS_SECONDS`].
    Histogram,
}

/// Who records a metric, and so from when a node exports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricScope {
    /// Any node, from its first HTTP request; recorded by `s4-api`.
    Http,
    /// Any node, from its start; recorded by `s4-server`.
    Node,
    /// Only a node running under an Enterprise licence; recorded by `s4-server`.
    License,
    /// A cluster node, from its start: [`register_cluster_metrics`] registers
    /// every series of it at zero.
    Cluster,
}

/// A label of a metric: its name and, when it takes only a closed set of
/// values, that set. `peer` and `pool` are open, and take the names of the
/// pool and its members.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogLabel {
    /// The label's name.
    pub name: &'static str,
    /// Every value the label takes, or `None` for an open label.
    pub values: Option<&'static [&'static str]>,
}

/// One metric of the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogEntry {
    /// The metric's name.
    pub name: &'static str,
    /// Its Prometheus type.
    pub kind: MetricKind,
    /// Its labels, in the order it is recorded with. The order is part of the
    /// series' identity to the recorder: a series registered with its labels
    /// in another order than the one it is recorded with is exported twice.
    pub labels: &'static [CatalogLabel],
    /// Who records it.
    pub scope: MetricScope,
    /// The `# HELP` text.
    pub help: &'static str,
}

const fn open(name: &'static str) -> CatalogLabel {
    CatalogLabel { name, values: None }
}

const fn closed(name: &'static str, values: &'static [&'static str]) -> CatalogLabel {
    CatalogLabel {
        name,
        values: Some(values),
    }
}

const REBALANCE_RANGE_STATES: &[&str] = &["pending", "streaming", "verified", "cut_over"];
const REBALANCE_PAUSE_REASONS: &[&str] = &["operator", "no_room"];
const REBALANCE_ERROR_REASONS: &[&str] = &[
    "checksum",
    "unavailable",
    "ambiguous",
    "storage",
    "placement",
];
const QUORUM_OPERATIONS: &[&str] = &["write", "read", "head", "delete", "list", "multipart"];
const QUORUM_ORIGINS: &[&str] = &["client", "system"];
const REPAIR_OUTCOMES: &[&str] = &["ok", "failed"];
const ANTI_ENTROPY_ROUND_OUTCOMES: &[&str] = &["complete", "incomplete", "skipped"];
const ANTI_ENTROPY_REPAIR_OUTCOMES: &[&str] = &["ok", "failed", "ambiguous"];
const UNENFORCED_CONFIG_KINDS: &[&str] = &["versioning", "object_lock"];
const IAM_AUTH_REFUSAL_REASONS: &[&str] = &["starting", "stale"];

/// The IAM settings `s4_iam_config_mismatch` compares.
const IAM_CONFIG_KINDS: &[&str] = &["root", "jwt_secret", "encryption_key"];
const CONFIG_ADOPTION_OUTCOMES: &[&str] = &["adopted", "superseded", "orphaned", "deferred"];
const BACKFILL_KEY_OUTCOMES: &[&str] = &["copied", "already", "unconfirmed", "failed", "dropped"];
const BACKFILL_PASS_OUTCOMES: &[&str] = &["complete", "incomplete"];

/// Every metric S4 exports outside the erasure-coding family, the one list
/// its registration, its descriptions and its checks are written from
/// (plan `EXTENDED-METRICS.md`). `scripts/tests/cluster-metrics-catalog.txt`
/// is the same list for scenario 55; a test holds the two equal.
pub const METRIC_CATALOG: &[CatalogEntry] = &[
    CatalogEntry {
        name: "http_requests_total",
        kind: MetricKind::Counter,
        labels: &[open("method"), open("path"), open("status")],
        scope: MetricScope::Http,
        help: "HTTP requests this node answered, counted when the answer is complete.",
    },
    CatalogEntry {
        name: "http_request_duration_seconds",
        kind: MetricKind::Histogram,
        labels: &[open("method"), open("path")],
        scope: MetricScope::Http,
        help: "How long this node took to answer an HTTP request.",
    },
    CatalogEntry {
        name: "s4_build_info",
        kind: MetricKind::Gauge,
        labels: &[open("version"), closed("edition", &["ce", "ee"])],
        scope: MetricScope::Node,
        help: "Always 1: the version of this node and the edition it runs as.",
    },
    CatalogEntry {
        name: "s4_license_expiry_timestamp_seconds",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::License,
        help: "When the Enterprise licence this node runs under expires, in Unix seconds.",
    },
    CatalogEntry {
        name: "s4_cluster_migration_pending_records",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Legacy single-node migration obligations awaiting every current owner.",
    },
    CatalogEntry {
        name: "s4_cluster_migration_records_total",
        kind: MetricKind::Counter,
        labels: &[closed("outcome", &["migrated", "superseded"])],
        scope: MetricScope::Cluster,
        help: "Legacy migration obligations completed on every current owner during this process.",
    },
    CatalogEntry {
        name: "s4_cluster_nodes",
        kind: MetricKind::Gauge,
        labels: &[
            open("pool"),
            closed("status", &["alive", "suspect", "dead", "left"]),
        ],
        scope: MetricScope::Cluster,
        help: "Configured members of the pool by status, as this node sees them, itself included.",
    },
    CatalogEntry {
        name: "s4_cluster_peer_up",
        kind: MetricKind::Gauge,
        labels: &[open("peer")],
        scope: MetricScope::Cluster,
        help: "1 while this node holds the peer alive, 0 otherwise.",
    },
    CatalogEntry {
        name: "s4_cluster_epoch",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Topology epoch of this node.",
    },
    CatalogEntry {
        name: "s4_quorum_operations_total",
        kind: MetricKind::Counter,
        labels: &[
            closed("operation", QUORUM_OPERATIONS),
            closed("outcome", &["ok", "not_found", "quorum_not_met", "error"]),
            closed("origin", QUORUM_ORIGINS),
        ],
        scope: MetricScope::Cluster,
        help: "Quorum operations this node coordinated, by how they ended and whom they served: \
               a request to this node (client) or the node's own work (system).",
    },
    CatalogEntry {
        name: "s4_quorum_duration_seconds",
        kind: MetricKind::Histogram,
        labels: &[
            closed("operation", QUORUM_OPERATIONS),
            closed("origin", QUORUM_ORIGINS),
        ],
        scope: MetricScope::Cluster,
        help: "How long a quorum operation this node coordinated took.",
    },
    CatalogEntry {
        name: "s4_peer_requests_total",
        kind: MetricKind::Counter,
        labels: &[
            open("peer"),
            closed(
                "outcome",
                &["ok", "error", "timeout", "skipped", "cancelled"],
            ),
        ],
        scope: MetricScope::Cluster,
        help: "Calls from this node to a peer, one per call after its retries, by how they ended.",
    },
    CatalogEntry {
        name: "s4_peer_request_duration_seconds",
        kind: MetricKind::Histogram,
        labels: &[open("peer")],
        scope: MetricScope::Cluster,
        help: "How long a call to a peer took, for calls that got an answer or an error.",
    },
    CatalogEntry {
        name: "s4_read_repairs_total",
        kind: MetricKind::Counter,
        labels: &[closed("outcome", REPAIR_OUTCOMES)],
        scope: MetricScope::Cluster,
        help: "Writes to a stale replica after a quorum read, by whether the replica took them.",
    },
    CatalogEntry {
        name: "s4_hints_pending",
        kind: MetricKind::Gauge,
        labels: &[open("peer")],
        scope: MetricScope::Cluster,
        help: "Hints this node holds on disk for the peer.",
    },
    CatalogEntry {
        name: "s4_hints_stored_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Hints written to the hint log.",
    },
    CatalogEntry {
        name: "s4_hints_delivered_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Hints after which their replica was at the current state of the key.",
    },
    CatalogEntry {
        name: "s4_hints_dropped_total",
        kind: MetricKind::Counter,
        labels: &[closed(
            "reason",
            &["full", "expired", "rejected", "superseded", "ambiguous"],
        )],
        scope: MetricScope::Cluster,
        help: "Hints that will not be delivered, by why.",
    },
    CatalogEntry {
        name: "s4_anti_entropy_rounds_total",
        kind: MetricKind::Counter,
        labels: &[closed("outcome", ANTI_ENTROPY_ROUND_OUTCOMES)],
        scope: MetricScope::Cluster,
        help: "Anti-entropy rounds this node started, by how they ended: every key of the pair \
               settled, not all of them, or skipped for want of a ready pool or a live peer.",
    },
    CatalogEntry {
        name: "s4_anti_entropy_round_duration_seconds",
        kind: MetricKind::Histogram,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "How long an anti-entropy round this node ran took, skipped rounds aside.",
    },
    CatalogEntry {
        name: "s4_anti_entropy_divergent_keys_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help:
            "Keys this node's anti-entropy rounds found in different versions on it and its peer.",
    },
    CatalogEntry {
        name: "s4_anti_entropy_repairs_total",
        kind: MetricKind::Counter,
        labels: &[closed("outcome", ANTI_ENTROPY_REPAIR_OUTCOMES)],
        scope: MetricScope::Cluster,
        help: "Divergent keys, by what settling them came to: every replica that answered is at \
               the newest version, a replica was not brought to it, or the newest copy may have \
               missed a delete and nothing was written.",
    },
    CatalogEntry {
        name: "s4_repair_frontier_lag_seconds",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help:
            "Age of the oldest boundary of the repair frontier, as of the last anti-entropy round.",
    },
    CatalogEntry {
        name: "s4_tombstones_active",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Tombstones left after the last tombstone GC pass.",
    },
    CatalogEntry {
        name: "s4_tombstones_purged_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Tombstones tombstone GC removed.",
    },
    CatalogEntry {
        name: "s4_bucket_unenforced_configs",
        kind: MetricKind::Gauge,
        labels: &[closed("kind", UNENFORCED_CONFIG_KINDS)],
        scope: MetricScope::Cluster,
        help: "Bucket configurations this node stores and cluster mode does not enforce: \
               versioning and Object Lock accepted before cluster mode refused them.",
    },
    CatalogEntry {
        name: "s4_bucket_config_adoption_keys_total",
        kind: MetricKind::Counter,
        labels: &[closed("outcome", CONFIG_ADOPTION_OUTCOMES)],
        scope: MetricScope::Cluster,
        help: "Bucket configurations an earlier release stored on this node alone, by what \
               adopting them by the quorums came to: copied to the replicas behind, a replica \
               held a newer version, the bucket was deleted since, or left for the next pass.",
    },
    CatalogEntry {
        name: "s4_bucket_config_quorum_reads",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "1 when this node reads bucket configurations through the quorums without \
               local fallback; 0 during its initial legacy adoption.",
    },
    CatalogEntry {
        name: "s4_bucket_metadata_stage",
        kind: MetricKind::Gauge,
        labels: &[open("pool")],
        scope: MetricScope::Cluster,
        help: "Stage of this node in the move of the pool's bucket records to more replicas: 0 \
               legacy, 1 dual write, 2 read new, 3 settled.",
    },
    CatalogEntry {
        name: "s4_bucket_metadata_replication_factor",
        kind: MetricKind::Gauge,
        labels: &[open("pool")],
        scope: MetricScope::Cluster,
        help: "Replicas the pool's bucket records and settings are kept on once their move \
               settled.",
    },
    CatalogEntry {
        name: "s4_bucket_metadata_backfill_keys_total",
        kind: MetricKind::Counter,
        labels: &[closed("outcome", BACKFILL_KEY_OUTCOMES)],
        scope: MetricScope::Cluster,
        help: "Bucket records and settings the backfill of this node went through, by what it \
               came to: brought to a new replica, already on every replica, left as they were \
               because no acknowledged write made them, not settled, or — markers this node \
               made up outside the old layout — removed.",
    },
    CatalogEntry {
        name: "s4_bucket_metadata_backfill_passes_total",
        kind: MetricKind::Counter,
        labels: &[closed("outcome", BACKFILL_PASS_OUTCOMES)],
        scope: MetricScope::Cluster,
        help: "Backfill passes of this node, by whether every key was settled.",
    },
    CatalogEntry {
        name: "s4_pool_generation_fenced_writes_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help:
            "Mutations refused because the coordinator used an obsolete pool placement generation.",
    },
    CatalogEntry {
        name: "s4_pool_generation",
        kind: MetricKind::Gauge,
        labels: &[open("pool")],
        scope: MetricScope::Cluster,
        help: "Epoch of the pool generation this node places keys by: while the pool grows, the \
               one its keys move from; once it settled, the grown one.",
    },
    CatalogEntry {
        name: "s4_rebalance_ranges",
        kind: MetricKind::Gauge,
        labels: &[closed("state", REBALANCE_RANGE_STATES)],
        scope: MetricScope::Cluster,
        help: "Ranges of the ring of a growing pool by state on this node: not begun, being \
               moved, verified, read through the grown generation. All 0 while the pool does \
               not grow.",
    },
    CatalogEntry {
        name: "s4_rebalance_keys_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Keys this node brought to the new owners of a growing pool.",
    },
    CatalogEntry {
        name: "s4_rebalance_bytes_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Bytes of copies this node sent to the new owners of a growing pool.",
    },
    CatalogEntry {
        name: "s4_rebalance_remaining_bytes",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Estimate of what this node has still to go through before it handed a growing \
               pool's keys to their new owners: its stored bytes in the share of its ranges not \
               handed over yet. 0 once it handed over everything, or while the pool does not \
               grow.",
    },
    CatalogEntry {
        name: "s4_rebalance_paused",
        kind: MetricKind::Gauge,
        labels: &[closed("reason", REBALANCE_PAUSE_REASONS)],
        scope: MetricScope::Cluster,
        help: "1 while the move of this node's keys to the new owners is paused by an operator \
               (operator), or waits for a new owner short of space (no_room).",
    },
    CatalogEntry {
        name: "s4_rebalance_errors_total",
        kind: MetricKind::Counter,
        labels: &[closed("reason", REBALANCE_ERROR_REASONS)],
        scope: MetricScope::Cluster,
        help: "Keys of a growing pool this node could not bring to the new owners on a try, by \
               reason: a copy that failed its checksum, a replica that did not answer, a newest \
               version no quorum confirmed, this node's storage, the placement. Each is tried \
               again.",
    },
    CatalogEntry {
        name: "s4_bucket_metadata_fenced_writes_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Writes of bucket records and settings this node refused because their \
               coordinator placed them by a stage too old: a node of an earlier release, or \
               one behind the pool.",
    },
    CatalogEntry {
        name: "s4_iam_fenced_writes_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Writes of users and root's state this node refused because they were made \
               outside the current lease of IAM changes: by a node whose lease ran out or was \
               taken over by a later one, or without a lease at all.",
    },
    CatalogEntry {
        name: "s4_iam_epoch_applied",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Wall time, in Unix milliseconds, of the HLC of the newest change of users or \
               root's state in this node's copy of IAM, deletes included; 0 while it holds none.",
    },
    CatalogEntry {
        name: "s4_iam_replica_lag_seconds",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "How long ago a majority of the pool last agreed with this node's copy of IAM; \
               since the node started while no majority has yet. IAM credentials are refused \
               past S4_IAM_MAX_STALENESS_SECS.",
    },
    CatalogEntry {
        name: "s4_iam_auth_refused_total",
        kind: MetricKind::Counter,
        labels: &[closed("reason", IAM_AUTH_REFUSAL_REASONS)],
        scope: MetricScope::Cluster,
        help: "Requests with IAM credentials this node answered 503 because no majority of the \
               pool had agreed with its copy of IAM: since it started (starting), or for longer \
               than S4_IAM_MAX_STALENESS_SECS (stale).",
    },
    CatalogEntry {
        name: "s4_iam_adoption_pending",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "1 while some node of the pool keeps IAM users of its own that the pool has not \
               taken over, and IAM changes are refused; 0 once every node announced none.",
    },
    CatalogEntry {
        name: "s4_iam_config_mismatch",
        kind: MetricKind::Gauge,
        labels: &[closed("kind", IAM_CONFIG_KINDS)],
        scope: MetricScope::Cluster,
        help: "1 while the live nodes of the pool are configured differently for IAM: root's \
               password (root), S4_JWT_SECRET (jwt_secret) or the IAM encryption key \
               (encryption_key); the cluster health names the nodes.",
    },
    CatalogEntry {
        name: "s4_iam_records_by_key_id",
        kind: MetricKind::Gauge,
        labels: &[open("key_id")],
        scope: MetricScope::Node,
        help: "IAM users whose S3 secret this node's copy holds sealed with each encryption \
               key, by key id; key_id=\"clear\" for secrets still in clear text. An old key may \
               be removed once no user is left under it.",
    },
    CatalogEntry {
        name: "s4_node_disk_total_bytes",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Size of the filesystem under the data directory; 0 when it could not be measured.",
    },
    CatalogEntry {
        name: "s4_node_disk_used_bytes",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Space in use on the filesystem under the data directory.",
    },
    CatalogEntry {
        name: "s4_scrubber_blobs_scanned_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Blobs the scrubber verified.",
    },
    CatalogEntry {
        name: "s4_scrubber_corruptions_found_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Corrupt blobs the scrubber found.",
    },
    CatalogEntry {
        name: "s4_scrubber_corruptions_healed_total",
        kind: MetricKind::Counter,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "Corrupt blobs the scrubber healed from a replica.",
    },
    CatalogEntry {
        name: "s4_scrubber_scan_progress",
        kind: MetricKind::Gauge,
        labels: &[],
        scope: MetricScope::Cluster,
        help: "How far the current scrubber scan has got, from 0 to 1.",
    },
];

/// Gives every metric of the catalog its `# HELP` text.
///
/// Called once the recorder is installed, in any mode: a description alone
/// exports nothing, so a metric a node never records stays absent.
pub fn describe_metrics() {
    for entry in METRIC_CATALOG {
        match entry.kind {
            MetricKind::Counter => metrics::describe_counter!(entry.name, entry.help),
            MetricKind::Gauge => metrics::describe_gauge!(entry.name, entry.help),
            MetricKind::Histogram => metrics::describe_histogram!(entry.name, entry.help),
        }
    }
}

/// Registers every series of the cluster metrics at zero, so that each exists
/// from the node's start and an empty graph means nothing happened, not that
/// nothing is known. `peers` are the names of the other members of `pool` as
/// `S4_POOL_NODES` gives them; `epoch` is this node's topology epoch.
///
/// Called before any cluster service records anything: a gauge set to zero
/// here after a service had set it would lose what the service said.
pub fn register_cluster_metrics(pool: &str, peers: &[String], epoch: u64) {
    for entry in METRIC_CATALOG.iter().filter(|entry| entry.scope == MetricScope::Cluster) {
        for labels in series_of(entry, pool, peers) {
            match entry.kind {
                MetricKind::Counter => metrics::counter!(entry.name, labels).increment(0),
                MetricKind::Gauge => metrics::gauge!(entry.name, labels).set(0.0),
                MetricKind::Histogram => {
                    // Registering the handle is what makes the series exist.
                    let _ = metrics::histogram!(entry.name, labels);
                }
            }
        }
    }
    metrics::gauge!("s4_cluster_epoch").set(epoch as f64);
}

/// The label sets of every series of `entry`: each value of each closed label,
/// `pool` for the pool and each of `peers` for a peer. An open label that is
/// neither has values nobody can list in advance, and gets no series.
fn series_of(entry: &CatalogEntry, pool: &str, peers: &[String]) -> Vec<Vec<metrics::Label>> {
    let mut series = vec![Vec::new()];
    for label in entry.labels {
        let values: Vec<String> = match (label.values, label.name) {
            (Some(values), _) => values.iter().map(|value| (*value).to_owned()).collect(),
            (None, "peer") => peers.to_vec(),
            (None, "pool") => vec![pool.to_owned()],
            (None, _) => return Vec::new(),
        };
        series = series
            .into_iter()
            .flat_map(|labels| {
                values.iter().map(move |value| {
                    let mut labels = labels.clone();
                    labels.push(metrics::Label::new(label.name, value.clone()));
                    labels
                })
            })
            .collect();
    }
    series
}

// ---------------------------------------------------------------------------
// Quorum operations
// ---------------------------------------------------------------------------

/// Whom a quorum operation served, as its `origin` label names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuorumOrigin {
    /// A request this node took over HTTP: S3 or the admin API.
    Client,
    /// The node's own work: publishing its records, moving a grown pool's
    /// keys, workers, repair.
    System,
}

impl QuorumOrigin {
    fn label(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::System => "system",
        }
    }
}

tokio::task_local! {
    static QUORUM_ORIGIN: QuorumOrigin;
}

/// Runs `work` as the serving of a request: every quorum operation it
/// coordinates is counted with `origin="client"`.
///
/// Anything outside such a scope is the node's own work and counted as
/// `system`, which keeps what clients cost apart from what the node does by
/// itself, as client request metrics are kept apart in other stores. The scope
/// belongs to the task that runs `work`: a task spawned from it is the node's.
pub async fn on_behalf_of_client<F: Future>(work: F) -> F::Output {
    QUORUM_ORIGIN.scope(QuorumOrigin::Client, work).await
}

fn quorum_origin() -> QuorumOrigin {
    QUORUM_ORIGIN.try_with(|origin| *origin).unwrap_or(QuorumOrigin::System)
}

/// What a quorum operation did, as its `operation` label names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuorumOperation {
    /// Object data or a namespace entry written to the replicas.
    Write,
    /// An object read, whole or as a stream.
    Read,
    /// Object metadata read.
    Head,
    /// An object removed from the replicas.
    Delete,
    /// A listing gathered from the pool.
    List,
    /// A step of a multipart upload.
    Multipart,
}

impl QuorumOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::Read => "read",
            Self::Head => "head",
            Self::Delete => "delete",
            Self::List => "list",
            Self::Multipart => "multipart",
        }
    }
}

/// The `outcome` label of a finished quorum operation.
///
/// An absent object is its own outcome: a read that finds nothing is an
/// answer, not a failure of the quorum. A timeout is `quorum_not_met` — the
/// replicas did not reach the quorum in time.
fn quorum_outcome<T>(result: &Result<T, ClusterError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(ClusterError::ObjectNotFound { .. } | ClusterError::MultipartUploadNotFound { .. }) => {
            "not_found"
        }
        Err(
            ClusterError::QuorumNotMet { .. }
            | ClusterError::WriteTimeout(_)
            | ClusterError::ReadTimeout(_),
        ) => "quorum_not_met",
        Err(_) => "error",
    }
}

/// Runs one quorum operation and records how it ended and how long it took.
///
/// Each public operation of a coordinator runs through here exactly once, so
/// `s4_quorum_operations_total` counts what the coordinators were asked to do.
pub(crate) async fn observe_quorum<T>(
    operation: QuorumOperation,
    work: impl Future<Output = Result<T, ClusterError>>,
) -> Result<T, ClusterError> {
    let started = Instant::now();
    let in_flight = QuorumInFlight::enter();
    let result = work.await;
    drop(in_flight);
    let label = operation.label();
    let origin = quorum_origin().label();
    metrics::counter!(
        "s4_quorum_operations_total",
        "operation" => label,
        "outcome" => quorum_outcome(&result),
        "origin" => origin
    )
    .increment(1);
    metrics::histogram!("s4_quorum_duration_seconds", "operation" => label, "origin" => origin)
        .record(started.elapsed().as_secs_f64());
    result
}

/// Quorum operations this node coordinates right now: the clients' traffic,
/// which background work such as the move of a grown pool's keys gives way to.
static QUORUM_OPERATIONS_IN_FLIGHT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many quorum operations this node coordinates right now.
pub fn quorum_operations_in_flight() -> u64 {
    QUORUM_OPERATIONS_IN_FLIGHT.load(std::sync::atomic::Ordering::Relaxed)
}

/// Counts one quorum operation in flight until dropped, however it ends.
struct QuorumInFlight;

impl QuorumInFlight {
    fn enter() -> Self {
        QUORUM_OPERATIONS_IN_FLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self
    }
}

impl Drop for QuorumInFlight {
    fn drop(&mut self) {
        QUORUM_OPERATIONS_IN_FLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Calls to peers
// ---------------------------------------------------------------------------

/// The name each peer carries in the `peer` label.
///
/// Bootstrap fills it from `S4_POOL_NODES` once it has matched every
/// configured member to the identity that answers for it. The configured name
/// is the one an operator wrote and the one that survives a node being
/// replaced; the name a node announces in gossip is a placeholder until its
/// first metadata arrives, which would split one peer over two series. A node
/// outside the configured pool is labelled by its identity.
#[derive(Debug, Default)]
pub struct PeerNames {
    names: RwLock<HashMap<NodeId, String>>,
}

impl PeerNames {
    /// Names the given identities, replacing whatever was known before.
    pub fn set(&self, names: impl IntoIterator<Item = (NodeId, String)>) {
        let mut known = self.names.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        *known = names.into_iter().collect();
    }

    /// The `peer` label of `node_id`.
    pub(crate) fn label(&self, node_id: NodeId) -> String {
        self.names
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&node_id)
            .cloned()
            .unwrap_or_else(|| node_id.to_string())
    }
}

/// How one call to a peer ended, as its `outcome` label names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerOutcome {
    /// The peer answered.
    Ok,
    /// The call failed: the peer could not be reached or refused it.
    Error,
    /// The peer took the call and did not answer within the request timeout.
    Timeout,
    /// Gossip holds the peer dead, so no call was made.
    Skipped,
}

impl PeerOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::Skipped => "skipped",
        }
    }
}

/// One call to a peer, recorded exactly once however it ends.
///
/// A call nobody waits for any more is dropped before it finishes: a quorum
/// read lets go of the replicas it no longer needs, and a coordinator stops
/// waiting for a replica at its own deadline. Such a call is recorded as
/// `cancelled` when the guard is dropped. Without that, calls to a peer that
/// hangs would not show up anywhere — neither as answered nor as failed.
///
/// The duration goes into the histogram only for a call that got an answer or
/// an error; a skipped call took no time and a cancelled one has no end.
pub(crate) struct PeerCall {
    peer: String,
    started: Instant,
    finished: bool,
}

impl PeerCall {
    /// Starts timing a call to the peer labelled `peer`.
    pub(crate) fn start(peer: String) -> Self {
        Self {
            peer,
            started: Instant::now(),
            finished: false,
        }
    }

    /// Records how the call ended.
    pub(crate) fn finish(mut self, outcome: PeerOutcome) {
        self.finished = true;
        metrics::counter!(
            "s4_peer_requests_total",
            "peer" => self.peer.clone(),
            "outcome" => outcome.label()
        )
        .increment(1);
        if outcome != PeerOutcome::Skipped {
            metrics::histogram!("s4_peer_request_duration_seconds", "peer" => self.peer.clone())
                .record(self.started.elapsed().as_secs_f64());
        }
    }
}

impl Drop for PeerCall {
    fn drop(&mut self) {
        if !self.finished {
            metrics::counter!(
                "s4_peer_requests_total",
                "peer" => self.peer.clone(),
                "outcome" => "cancelled"
            )
            .increment(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Membership
// ---------------------------------------------------------------------------

/// The pool whose membership a node reports: the pool's name, the node itself
/// and every member `S4_POOL_NODES` configures, under the name written there.
///
/// Membership is counted over this roster and not over whatever gossip happens
/// to hold. Gossip holds a node outside the pool as readily as one inside it,
/// and holds nothing at all for a configured member it has never heard from —
/// which is exactly the member an operator has to see as down.
#[derive(Debug, Clone)]
pub struct PoolRoster {
    pool: String,
    local: NodeId,
    members: Vec<(NodeId, String)>,
}

impl PoolRoster {
    /// The roster of pool `pool` as `local` reports it; `members` are the
    /// configured members, `local` among them, with their configured names.
    pub fn new(
        pool: impl Into<String>,
        local: NodeId,
        members: impl IntoIterator<Item = (NodeId, String)>,
    ) -> Self {
        Self {
            pool: pool.into(),
            local,
            members: members.into_iter().collect(),
        }
    }

    /// The name of the pool.
    pub fn pool(&self) -> &str {
        &self.pool
    }

    /// The configured name of `node`, if it is a member.
    pub fn name_of(&self, node: NodeId) -> Option<&str> {
        self.members.iter().find(|(id, _)| *id == node).map(|(_, name)| name.as_str())
    }

    /// How every configured member stands in the member map as this node holds
    /// it now, in the order `S4_POOL_NODES` lists them. The metrics and the
    /// admin API both count by this, so the two cannot disagree on the rule.
    ///
    /// This node is alive: it is the one answering. A configured member the map
    /// holds nothing for is dead — this node has never heard from it since it
    /// started, or has forgotten where it is — so the members of one pool
    /// always add up to its configured size.
    pub fn statuses<'a>(
        &'a self,
        members: &'a HashMap<NodeId, NodeState>,
    ) -> impl Iterator<Item = MemberStatus<'a>> + 'a {
        self.members.iter().map(move |(node_id, name)| {
            let local = *node_id == self.local;
            let status = if local {
                NodeStatus::Alive
            } else {
                members.get(node_id).map_or(NodeStatus::Dead, |state| state.status)
            };
            MemberStatus {
                node_id: *node_id,
                name,
                local,
                status,
            }
        })
    }
}

/// One configured member of a pool and how this node sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemberStatus<'a> {
    /// The member's identity.
    pub node_id: NodeId,
    /// Its name in `S4_POOL_NODES`.
    pub name: &'a str,
    /// Whether it is the node answering.
    pub local: bool,
    /// Its liveness.
    pub status: NodeStatus,
}

/// Sets `s4_cluster_nodes` and `s4_cluster_peer_up` from the member map as
/// this node holds it now, by [`PoolRoster::statuses`].
pub(crate) fn publish_membership(roster: &PoolRoster, members: &HashMap<NodeId, NodeState>) {
    let mut counts = [0u32; NodeStatus::ALL.len()];
    for member in roster.statuses(members) {
        if let Some(slot) = NodeStatus::ALL.iter().position(|known| *known == member.status) {
            counts[slot] += 1;
        }
        if !member.local {
            let up = if member.status == NodeStatus::Alive {
                1.0
            } else {
                0.0
            };
            metrics::gauge!("s4_cluster_peer_up", "peer" => member.name.to_string()).set(up);
        }
    }
    for (status, count) in NodeStatus::ALL.iter().zip(counts) {
        metrics::gauge!(
            "s4_cluster_nodes",
            "pool" => roster.pool.clone(),
            "status" => status.as_str()
        )
        .set(f64::from(count));
    }
}

/// Sets the size of the filesystem under the data directory and how much of it
/// is in use, as this node last measured them for its peers. Zero is a
/// measurement that failed.
pub(crate) fn set_node_disk(total_bytes: u64, used_bytes: u64) {
    metrics::gauge!("s4_node_disk_total_bytes").set(total_bytes as f64);
    metrics::gauge!("s4_node_disk_used_bytes").set(used_bytes as f64);
}

// ---------------------------------------------------------------------------
// Read repair and hinted handoff
// ---------------------------------------------------------------------------

/// Records one read repair write to a stale replica and whether it landed.
pub(crate) fn record_read_repair(repaired: bool) {
    let outcome = if repaired { "ok" } else { "failed" };
    metrics::counter!("s4_read_repairs_total", "outcome" => outcome).increment(1);
}

/// Why a hint will never be delivered, as the `reason` label names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HintDropReason {
    /// The hint store for the target was full; the hint was never written.
    Full,
    /// The hint outlived its TTL before the target came back.
    Expired,
    /// The target refused what delivery sent it, or cannot take it.
    Rejected,
    /// The key left replication: it is erasure coded now, or the target no
    /// longer holds it.
    Superseded,
    /// The replicas disagree in a way only tombstones could settle: the
    /// newest copy may be one that missed a delete, and spreading it could
    /// bring the deleted key back. Nothing was written; read repair or
    /// anti-entropy is left to settle the key.
    Ambiguous,
}

/// Records `count` hints written to disk.
pub(crate) fn record_hints_stored(count: u64) {
    if count > 0 {
        metrics::counter!("s4_hints_stored_total").increment(count);
    }
}

/// Records `count` hints after which the target was at its key's current
/// state.
pub(crate) fn record_hints_delivered(count: u64) {
    if count > 0 {
        metrics::counter!("s4_hints_delivered_total").increment(count);
    }
}

/// Records `count` hints that will not reach their target; anti-entropy is
/// left to bring that replica up to date.
pub(crate) fn record_hints_dropped(reason: HintDropReason, count: u64) {
    if count == 0 {
        return;
    }
    let reason = match reason {
        HintDropReason::Full => "full",
        HintDropReason::Expired => "expired",
        HintDropReason::Rejected => "rejected",
        HintDropReason::Superseded => "superseded",
        HintDropReason::Ambiguous => "ambiguous",
    };
    metrics::counter!("s4_hints_dropped_total", "reason" => reason).increment(count);
}

/// Sets how many hints wait on disk for the peer labelled `peer`.
pub(crate) fn set_hints_pending(peer: &str, pending: u64) {
    metrics::gauge!("s4_hints_pending", "peer" => peer.to_owned()).set(pending as f64);
}

// ---------------------------------------------------------------------------
// Anti-entropy and tombstone GC
// ---------------------------------------------------------------------------

/// How an anti-entropy round ended, as the `outcome` label names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoundOutcome {
    /// Every leaf was compared and every divergent key settled.
    Complete,
    /// The peer stopped answering, a key was not settled, or the round
    /// reached its key limit.
    Incomplete,
    /// The pool was not ready for rounds, or no peer was alive.
    Skipped,
}

/// Records one anti-entropy round: how it ended, and how long a round that
/// ran took.
pub(crate) fn record_anti_entropy_round(outcome: RoundOutcome, took: std::time::Duration) {
    let label = match outcome {
        RoundOutcome::Complete => "complete",
        RoundOutcome::Incomplete => "incomplete",
        RoundOutcome::Skipped => "skipped",
    };
    metrics::counter!("s4_anti_entropy_rounds_total", "outcome" => label).increment(1);
    if outcome != RoundOutcome::Skipped {
        metrics::histogram!("s4_anti_entropy_round_duration_seconds").record(took.as_secs_f64());
    }
}

/// Sets the age of the oldest cut of the repair frontier over the peers of
/// the pool, as of the round just over.
pub(crate) fn record_repair_frontier_lag(seconds: f64) {
    metrics::gauge!("s4_repair_frontier_lag_seconds").set(seconds);
}

/// What settling one divergent key came to, as the `outcome` label names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyRepairOutcome {
    /// Every replica that answered is at the key's newest version.
    Ok,
    /// A replica was not brought to it, or did not answer.
    Failed,
    /// The newest copy may have missed a delete; nothing was written.
    Ambiguous,
}

/// Records one divergent key and what settling it came to.
pub(crate) fn record_anti_entropy_key(outcome: KeyRepairOutcome) {
    let label = match outcome {
        KeyRepairOutcome::Ok => "ok",
        KeyRepairOutcome::Failed => "failed",
        KeyRepairOutcome::Ambiguous => "ambiguous",
    };
    metrics::counter!("s4_anti_entropy_divergent_keys_total").increment(1);
    metrics::counter!("s4_anti_entropy_repairs_total", "outcome" => label).increment(1);
}

/// Records tombstones tombstone GC removed, as soon as each page of a pass
/// removed them: a pass that fails later does not lose them.
pub(crate) fn record_tombstones_purged(purged: u64) {
    metrics::counter!("s4_tombstones_purged_total").increment(purged);
}

/// Records what one pass of adopting bucket configurations came to, key by
/// key: `adopted`, `superseded` and `orphaned` keys are settled, `deferred`
/// ones — and listings that failed — are counted again by the next pass.
pub(crate) fn record_config_adoption(adopted: u64, superseded: u64, orphaned: u64, deferred: u64) {
    for (outcome, keys) in [
        ("adopted", adopted),
        ("superseded", superseded),
        ("orphaned", orphaned),
        ("deferred", deferred),
    ] {
        metrics::counter!("s4_bucket_config_adoption_keys_total", "outcome" => outcome)
            .increment(keys);
    }
}

/// Records whether this node reads bucket configurations through the quorums.
pub(crate) fn set_config_quorum_reads(open: bool) {
    metrics::gauge!("s4_bucket_config_quorum_reads").set(if open { 1.0 } else { 0.0 });
}

/// Sets the stage and the target replicas of the move of `pool`'s bucket
/// metadata, as this node is at it.
pub(crate) fn set_bucket_metadata_placement(
    pool: &str,
    record: &crate::metadata_placement::PlacementRecord,
) {
    metrics::gauge!("s4_bucket_metadata_stage", "pool" => pool.to_owned())
        .set(f64::from(record.stage.number()));
    metrics::gauge!("s4_bucket_metadata_replication_factor", "pool" => pool.to_owned())
        .set(f64::from(record.to.replication_factor));
}

/// Records one backfill pass: each key by what it came to, and whether the
/// pass settled every one.
pub(crate) fn record_backfill_pass(report: &crate::metadata_placement::BackfillReport) {
    for (outcome, keys) in [
        ("copied", report.copied),
        ("already", report.already),
        ("unconfirmed", report.unconfirmed),
        ("failed", report.failed),
        ("dropped", report.dropped),
    ] {
        metrics::counter!("s4_bucket_metadata_backfill_keys_total", "outcome" => outcome)
            .increment(keys);
    }
    let pass = if report.complete() {
        "complete"
    } else {
        "incomplete"
    };
    metrics::counter!("s4_bucket_metadata_backfill_passes_total", "outcome" => pass).increment(1);
}

/// Records one write of bucket metadata this replica fenced.
pub(crate) fn record_bucket_metadata_fenced_write() {
    metrics::counter!("s4_bucket_metadata_fenced_writes_total").increment(1);
}

/// Records one write of the cluster's control state this replica fenced.
pub(crate) fn record_iam_fenced_write() {
    metrics::counter!("s4_iam_fenced_writes_total").increment(1);
}

/// Records one request with IAM credentials refused because this node's copy
/// of IAM was not confirmed current, for `reason`.
pub(crate) fn record_iam_auth_refused(reason: &'static str) {
    metrics::counter!("s4_iam_auth_refused_total", "reason" => reason).increment(1);
}

/// Records whether IAM changes wait for the pool to take over the IAM some
/// node kept of its own.
pub(crate) fn set_iam_adoption_pending(pending: bool) {
    metrics::gauge!("s4_iam_adoption_pending").set(if pending { 1.0 } else { 0.0 });
}

/// Records whether the live nodes of the pool disagree on the IAM setting
/// `kind`.
pub(crate) fn set_iam_config_mismatch(kind: &'static str, mismatch: bool) {
    metrics::gauge!("s4_iam_config_mismatch", "kind" => kind).set(if mismatch { 1.0 } else { 0.0 });
}

/// Sets how many IAM users this node's copy holds under each encryption key
/// id; `gone` are the ids reported before that hold none now.
pub fn set_iam_records_by_key_id<'a>(
    held: impl IntoIterator<Item = (&'a str, usize)>,
    gone: impl IntoIterator<Item = &'a str>,
) {
    for key_id in gone {
        metrics::gauge!("s4_iam_records_by_key_id", "key_id" => key_id.to_string()).set(0.0);
    }
    for (key_id, users) in held {
        metrics::gauge!("s4_iam_records_by_key_id", "key_id" => key_id.to_string())
            .set(users as f64);
    }
}

/// Sets the epoch of this node's copy of IAM, as the wall time of its HLC in
/// milliseconds, and how long ago a majority last agreed with it.
pub(crate) fn set_iam_copy(epoch_wall_ms: u64, lag: std::time::Duration) {
    metrics::gauge!("s4_iam_epoch_applied").set(epoch_wall_ms as f64);
    metrics::gauge!("s4_iam_replica_lag_seconds").set(lag.as_secs_f64());
}

/// Records how many tombstones a pass of tombstone GC that went to the end
/// left.
pub(crate) fn record_tombstones_active(remaining: u64) {
    metrics::gauge!("s4_tombstones_active").set(remaining as f64);
}

// ---------------------------------------------------------------------------
// Scrubber
// ---------------------------------------------------------------------------

/// Update scrubber scan progress (0.0 .. 1.0).
pub(crate) fn set_scrubber_scan_progress(progress: f64) {
    metrics::gauge!("s4_scrubber_scan_progress").set(progress);
}

/// Record scrubber corruption found.
pub(crate) fn record_scrubber_corruption_found() {
    metrics::counter!("s4_scrubber_corruptions_found_total").increment(1);
}

/// Record scrubber corruption healed.
pub(crate) fn record_scrubber_corruption_healed() {
    metrics::counter!("s4_scrubber_corruptions_healed_total").increment(1);
}

/// Record scrubber blobs scanned.
pub(crate) fn record_scrubber_blobs_scanned(count: u64) {
    metrics::counter!("s4_scrubber_blobs_scanned_total").increment(count);
}

/// Count a mutation refused by pool placement generation fencing.
pub(crate) fn record_pool_generation_fenced_write() {
    metrics::counter!("s4_pool_generation_fenced_writes_total").increment(1);
}

// ---------------------------------------------------------------------------
// Pool growth
// ---------------------------------------------------------------------------

/// Records one key brought to the new owners of a growing pool, with the
/// bytes of the copies it took.
pub(crate) fn record_rebalance_key_moved(bytes: u64) {
    metrics::counter!("s4_rebalance_keys_total").increment(1);
    metrics::counter!("s4_rebalance_bytes_total").increment(bytes);
}

/// Records `count` keys of a growing pool that failed to move for `reason`.
pub(crate) fn record_rebalance_errors(reason: &'static str, count: u64) {
    metrics::counter!("s4_rebalance_errors_total", "reason" => reason).increment(count);
}

/// Where the growth of `pool` stands on this node, as the gauges show it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PoolGrowthGauges {
    /// Epoch of the generation this node places keys by.
    pub epoch: u64,
    /// Ranges by state; all zero while the pool does not grow.
    pub ranges: crate::placement::RangeCounts,
    /// Estimate of the bytes this node has still to go through.
    pub remaining_bytes: u64,
    /// An operator paused the move.
    pub paused: bool,
    /// The move waits for a new owner short of space.
    pub no_room: bool,
}

/// Sets the gauges of the growth of `pool` on this node.
pub(crate) fn set_pool_growth(pool: &str, gauges: &PoolGrowthGauges) {
    metrics::gauge!("s4_pool_generation", "pool" => pool.to_owned()).set(gauges.epoch as f64);
    for (state, count) in [
        ("pending", gauges.ranges.pending),
        ("streaming", gauges.ranges.streaming),
        ("verified", gauges.ranges.verified),
        ("cut_over", gauges.ranges.cut_over),
    ] {
        metrics::gauge!("s4_rebalance_ranges", "state" => state).set(count as f64);
    }
    metrics::gauge!("s4_rebalance_remaining_bytes").set(gauges.remaining_bytes as f64);
    let flag = |on: bool| if on { 1.0 } else { 0.0 };
    metrics::gauge!("s4_rebalance_paused", "reason" => "operator").set(flag(gauges.paused));
    metrics::gauge!("s4_rebalance_paused", "reason" => "no_room").set(flag(gauges.no_room));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Catalog and registration
    // -----------------------------------------------------------------------

    use crate::test_metrics::TestMetrics;

    fn peers() -> Vec<String> {
        vec!["node-2".to_string(), "node-3".to_string()]
    }

    /// `scripts/tests/cluster-metrics-catalog.txt`, read the way scenario 55
    /// reads it: `(kind, [(label, closed values or None)])` by name, and the
    /// bucket bounds. `None` in a checkout without `scripts/`.
    #[allow(clippy::type_complexity)]
    fn scenario_catalog() -> Option<(
        HashMap<String, (String, Vec<(String, Option<Vec<String>>)>)>,
        Vec<f64>,
    )> {
        let scripts = concat!(env!("CARGO_MANIFEST_DIR"), "/../scripts");
        if !std::path::Path::new(scripts).is_dir() {
            return None;
        }
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../scripts/tests/cluster-metrics-catalog.txt"
        );
        let text = std::fs::read_to_string(path).expect("the scenario's catalog is readable");
        let mut metrics = HashMap::new();
        let mut buckets = Vec::new();
        for line in text.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(bounds) = line.strip_prefix("@buckets ") {
                buckets = bounds.split(',').map(|b| b.parse().unwrap()).collect();
                continue;
            }
            let words: Vec<&str> = line.split_whitespace().collect();
            let [name, kind, spec] = words[..] else {
                panic!("malformed catalog line: {line}");
            };
            let labels = if spec == "-" {
                Vec::new()
            } else {
                spec.split(',')
                    .map(|part| match part.split_once('=') {
                        Some((label, values)) => (
                            label.to_string(),
                            Some(values.split('|').map(str::to_string).collect()),
                        ),
                        None => (part.to_string(), None),
                    })
                    .collect()
            };
            assert!(
                metrics.insert(name.to_string(), (kind.to_string(), labels)).is_none(),
                "{name} is listed twice"
            );
        }
        Some((metrics, buckets))
    }

    fn kind_name(kind: MetricKind) -> &'static str {
        match kind {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }

    /// The catalog the node registers from and the one scenario 55 checks
    /// against are one list: same names, types, labels in the same order,
    /// same closed values, same buckets.
    ///
    /// The public mirror on GitHub is published without `scripts/`
    /// (`.githubignore`), so there is no scenario there to agree with. Wherever
    /// `scripts/` exists, a missing catalog fails the test.
    #[test]
    fn migration_metrics_are_registered_with_closed_outcomes() {
        let pending = METRIC_CATALOG
            .iter()
            .find(|entry| entry.name == "s4_cluster_migration_pending_records")
            .expect("migration pending metric");
        assert_eq!(pending.kind, MetricKind::Gauge);
        assert!(pending.labels.is_empty());
        let records = METRIC_CATALOG
            .iter()
            .find(|entry| entry.name == "s4_cluster_migration_records_total")
            .expect("migration records metric");
        assert_eq!(records.kind, MetricKind::Counter);
        assert_eq!(
            records.labels,
            &[closed("outcome", &["migrated", "superseded"])]
        );
    }

    #[test]
    fn the_catalog_in_code_is_the_one_scenario_55_checks() {
        let Some((file, buckets)) = scenario_catalog() else {
            eprintln!("no scripts/ in this checkout, as in the public mirror: nothing to compare");
            return;
        };
        assert_eq!(buckets, DURATION_BUCKETS_SECONDS);

        let mut in_code = HashMap::new();
        for entry in METRIC_CATALOG {
            let labels: Vec<(String, Option<Vec<String>>)> = entry
                .labels
                .iter()
                .map(|label| {
                    (
                        label.name.to_string(),
                        label.values.map(|values| values.iter().map(|v| v.to_string()).collect()),
                    )
                })
                .collect();
            assert!(
                in_code
                    .insert(
                        entry.name.to_string(),
                        (kind_name(entry.kind).to_string(), labels)
                    )
                    .is_none(),
                "{} is listed twice",
                entry.name
            );
        }
        let mut names: Vec<&String> = in_code.keys().chain(file.keys()).collect();
        names.sort();
        names.dedup();
        for name in names {
            assert_eq!(
                in_code.get(name),
                file.get(name),
                "{name}: code (left) and file (right)"
            );
        }
    }

    /// Right after registration every cluster metric is there — its type, its
    /// help, every series at zero, a peer series for each peer — before
    /// anything has been recorded. The epoch is the one exception to zero.
    #[test]
    fn registration_makes_every_cluster_series_exist_at_zero() {
        let metrics = TestMetrics::install();
        describe_metrics();
        register_cluster_metrics("pool-1", &peers(), 7);

        for entry in METRIC_CATALOG.iter().filter(|e| e.scope == MetricScope::Cluster) {
            let name = entry.name;
            assert_eq!(
                metrics.kind(name).as_deref(),
                Some(kind_name(entry.kind)),
                "{name}"
            );
            assert_eq!(metrics.help(name).as_deref(), Some(entry.help), "{name}");
            let series = series_of(entry, "pool-1", &peers());
            assert!(!series.is_empty(), "{name} has no series to register");
            for labels in series {
                let labels: Vec<(&str, &str)> =
                    labels.iter().map(|l| (l.key(), l.value())).collect();
                let (sample, expected) = match entry.kind {
                    MetricKind::Histogram => (format!("{name}_count"), 0.0),
                    _ if name == "s4_cluster_epoch" => (name.to_string(), 7.0),
                    _ => (name.to_string(), 0.0),
                };
                assert_eq!(
                    metrics.value(&sample, &labels),
                    Some(expected),
                    "{name}{labels:?}"
                );
            }
        }
        for peer in peers() {
            assert_eq!(
                metrics.value(
                    "s4_peer_requests_total",
                    &[("peer", &peer), ("outcome", "ok")]
                ),
                Some(0.0)
            );
            assert_eq!(
                metrics.value("s4_cluster_peer_up", &[("peer", &peer)]),
                Some(0.0)
            );
        }
        assert_eq!(
            metrics.value(
                "s4_cluster_nodes",
                &[("pool", "pool-1"), ("status", "alive")]
            ),
            Some(0.0)
        );
        assert_eq!(
            metrics.kind("s4_build_info"),
            None,
            "registration is for cluster metrics; the node records its own"
        );
        assert!(metrics.repeated_series().is_empty());
    }

    /// Every value the code records a closed label with is one the catalog
    /// lists; a value outside it would be a series registration never made.
    #[test]
    fn the_code_records_closed_labels_only_with_listed_values() {
        let allowed = |metric: &str, label: &str| -> Vec<&'static str> {
            METRIC_CATALOG
                .iter()
                .find(|entry| entry.name == metric)
                .and_then(|entry| entry.labels.iter().find(|l| l.name == label))
                .and_then(|l| l.values)
                .unwrap_or_else(|| panic!("{metric} has no closed label {label}"))
                .to_vec()
        };
        for operation in [
            QuorumOperation::Write,
            QuorumOperation::Read,
            QuorumOperation::Head,
            QuorumOperation::Delete,
            QuorumOperation::List,
            QuorumOperation::Multipart,
        ] {
            assert!(allowed("s4_quorum_operations_total", "operation").contains(&operation.label()));
        }
        for result in [
            Ok(()),
            Err(ClusterError::QuorumNotMet {
                needed: 2,
                achieved: 1,
            }),
            Err(ClusterError::ObjectNotFound {
                bucket: "b".into(),
                key: "k".into(),
            }),
            Err(ClusterError::Rpc("x".into())),
        ] {
            assert!(
                allowed("s4_quorum_operations_total", "outcome").contains(&quorum_outcome(&result))
            );
        }
        for outcome in [
            PeerOutcome::Ok,
            PeerOutcome::Error,
            PeerOutcome::Timeout,
            PeerOutcome::Skipped,
        ] {
            assert!(allowed("s4_peer_requests_total", "outcome").contains(&outcome.label()));
        }
        assert!(allowed("s4_peer_requests_total", "outcome").contains(&"cancelled"));
        for origin in [QuorumOrigin::Client, QuorumOrigin::System] {
            assert!(allowed("s4_quorum_operations_total", "origin").contains(&origin.label()));
            assert!(allowed("s4_quorum_duration_seconds", "origin").contains(&origin.label()));
        }
        for status in NodeStatus::ALL {
            assert!(allowed("s4_cluster_nodes", "status").contains(&status.as_str()));
        }
    }

    /// Registration and recording name the same series: after both, no
    /// series is exported twice, and nothing outside the catalog is exported.
    /// Labels registered in another order than the one they are recorded with
    /// would show up here as a series exported twice.
    #[tokio::test]
    async fn recording_after_registration_adds_no_second_series() {
        let metrics = TestMetrics::install();
        register_cluster_metrics("pool-1", &peers(), 1);

        observe_quorum(QuorumOperation::Write, async { Ok::<_, ClusterError>(()) })
            .await
            .unwrap();
        PeerCall::start("node-2".into()).finish(PeerOutcome::Ok);
        PeerCall::start("node-3".into()).finish(PeerOutcome::Skipped);
        drop(PeerCall::start("node-3".into()));
        record_read_repair(true);
        record_hints_stored(1);
        record_hints_delivered(1);
        record_hints_dropped(HintDropReason::Expired, 1);
        set_hints_pending("node-2", 3);
        let local = NodeId(1);
        let roster = PoolRoster::new(
            "pool-1",
            local,
            [
                (local, "node-1".into()),
                (NodeId(2), "node-2".into()),
                (NodeId(3), "node-3".into()),
            ],
        );
        publish_membership(&roster, &HashMap::new());
        record_tombstones_purged(1);
        record_tombstones_active(2);
        set_node_disk(100, 40);
        set_scrubber_scan_progress(0.5);
        record_scrubber_blobs_scanned(1);
        record_scrubber_corruption_found();
        record_scrubber_corruption_healed();
        record_rebalance_key_moved(4096);
        record_rebalance_errors("checksum", 2);
        set_pool_growth(
            "pool-1",
            &PoolGrowthGauges {
                epoch: 2,
                ranges: crate::placement::RangeCounts {
                    pending: 5,
                    streaming: 2,
                    verified: 1,
                    cut_over: 3,
                },
                remaining_bytes: 777,
                paused: true,
                no_room: false,
            },
        );

        assert_eq!(metrics.repeated_series(), Vec::<String>::new());
        for family in metrics.families() {
            assert!(
                METRIC_CATALOG.iter().any(|entry| entry.name == family),
                "{family} is not in the catalog"
            );
        }
        assert_eq!(
            metrics.value("s4_hints_pending", &[("peer", "node-2")]),
            Some(3.0)
        );
        assert_eq!(
            metrics.value("s4_pool_generation", &[("pool", "pool-1")]),
            Some(2.0)
        );
        for (state, count) in [
            ("pending", 5.0),
            ("streaming", 2.0),
            ("verified", 1.0),
            ("cut_over", 3.0),
        ] {
            assert_eq!(
                metrics.value("s4_rebalance_ranges", &[("state", state)]),
                Some(count)
            );
        }
        assert_eq!(metrics.value("s4_rebalance_keys_total", &[]), Some(1.0));
        assert_eq!(metrics.value("s4_rebalance_bytes_total", &[]), Some(4096.0));
        assert_eq!(
            metrics.value("s4_rebalance_remaining_bytes", &[]),
            Some(777.0)
        );
        assert_eq!(
            metrics.value("s4_rebalance_paused", &[("reason", "operator")]),
            Some(1.0)
        );
        assert_eq!(
            metrics.value("s4_rebalance_paused", &[("reason", "no_room")]),
            Some(0.0)
        );
        assert_eq!(
            metrics.value("s4_rebalance_errors_total", &[("reason", "checksum")]),
            Some(2.0)
        );
        assert_eq!(
            metrics.value(
                "s4_cluster_nodes",
                &[("pool", "pool-1"), ("status", "dead")]
            ),
            Some(2.0)
        );
    }
}
