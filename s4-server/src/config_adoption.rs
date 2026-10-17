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

//! The move of a cluster node from bucket configurations stored on single
//! nodes to configurations held by the quorums.
//!
//! [`adopt`] passes over the configurations this node stored before
//! protocol 6 until one pass settled them all
//! ([`s4_cluster::ConfigAdoption`]), then records that durably and announces
//! it in gossip. A node that recorded it on an earlier start announces it at
//! once. [`open_gate`] watches gossip until every node of the pool announced
//! it, and then opens the quorum reads of configurations on this node and
//! records that too: a node restarted while a node of its pool is down reads
//! through the quorums at once ([`initial_gate`]). Neither sends a request of
//! its own to a peer: the announcement travels with the metadata gossip
//! spreads anyway.

use std::sync::Arc;
use std::time::Duration;

use s4_cluster::{
    ConfigAdoption, ConfigAdoptionStatus, ConfigReadGate, GossipHandle, NodeId, NodeStatus,
    BUCKET_CONFIG_MIN_PROTOCOL_VERSION,
};
use s4_core::storage::IndexDb;
use tokio::sync::watch;
use tracing::{info, warn};

/// How long a pass that left configurations for later waits for the next.
pub const RETRY_INTERVAL: Duration = Duration::from_secs(30);
/// Minimum time between the starts of adoption passes.
const MIN_PASS_INTERVAL: Duration = Duration::from_secs(1);
/// How often this node's view of the pool is read for the gate.
pub const GATE_INTERVAL: Duration = Duration::from_secs(1);
/// How many configurations left for later a warning names.
const NAMED: usize = 5;

/// Adopts this node's configurations, unless an earlier start did, and
/// announces it once they all are. Every pass that ended — and a start that
/// has nothing to pass over — bumps `passes`, for what counts this node's
/// configurations after adoption moved them. Stops early when `shutdown`
/// turns true.
pub async fn adopt(
    adoption: ConfigAdoption,
    index_db: Arc<IndexDb>,
    gossip: Arc<GossipHandle>,
    status: ConfigAdoptionStatus,
    passes: watch::Sender<u64>,
    mut shutdown: watch::Receiver<bool>,
) {
    // Subscribe before reading durable completion or starting any pass.
    let peers = gossip.subscribe_peer_changes();
    tokio::select! {
        biased;
        _ = shutdown_requested(&mut shutdown) => {}
        _ = run_adoption(adoption, index_db, gossip, status, passes, peers) => {}
    }
}

async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn run_adoption(
    adoption: ConfigAdoption,
    index_db: Arc<IndexDb>,
    gossip: Arc<GossipHandle>,
    status: ConfigAdoptionStatus,
    passes: watch::Sender<u64>,
    mut peers: watch::Receiver<()>,
) {
    match index_db.bucket_configs_adopted().await {
        Ok(true) => {
            status.mark_ready();
            gossip.announce_bucket_configs_adopted();
            passes.send_modify(|count| *count += 1);
            return;
        }
        Ok(false) => {}
        Err(e) => {
            warn!(error = %e, "could not read whether bucket configurations were adopted");
            status.waiting(
                None,
                vec![format!("could not read adoption completion: {e}")],
            );
        }
    }
    // Quorum reads are a monotone cutover, independent of whether this
    // node's most recent history is trusted. Reopening local fallback or
    // blindly readopting foreign copies could resurrect collected deletes.
    let adoption = match index_db.bucket_config_history_requires_confirmation().await {
        Ok(false) => adoption,
        Ok(true) => adoption.after_quorum_reads(),
        Err(e) => {
            warn!(error = %e, "could not read the configuration history marker; requiring confirmation");
            adoption.after_quorum_reads()
        }
    };
    loop {
        // Only consume notifications observed before this pass. An update
        // while the pass is running must remain pending for the next wait.
        peers.borrow_and_update();
        let next_pass = tokio::time::Instant::now() + MIN_PASS_INTERVAL;
        let report = adoption.pass().await;
        if report.is_complete() {
            match index_db.mark_bucket_configs_adopted().await {
                Ok(()) => {
                    status.mark_ready();
                    gossip.announce_bucket_configs_adopted();
                    passes.send_modify(|count| *count += 1);
                    info!(
                        adopted = report.adopted,
                        superseded = report.superseded,
                        orphaned = report.orphaned,
                        removed = report.removed,
                        "Bucket configurations stored before protocol 6 adopted by the quorums"
                    );
                    return;
                }
                Err(e) => {
                    warn!(error = %e, "could not record that bucket configurations were adopted");
                    status.waiting(
                        Some(0),
                        vec![format!("could not save adoption completion: {e}")],
                    );
                }
            }
        } else {
            let named: Vec<String> = report
                .later
                .iter()
                .take(NAMED)
                .map(|(key, why)| format!("{key}: {why}"))
                .collect();
            // A failed listing leaves the number of unenumerated keys unknown.
            let pending = (!report.later.iter().any(|(_, why)| why.starts_with("listing failed:")))
                .then_some(report.later.len() as u64);
            status.waiting(pending, named.clone());
            warn!(
                left = report.later.len(),
                adopted = report.adopted,
                first = ?named,
                retry_secs = RETRY_INTERVAL.as_secs(),
                "Bucket configurations left for a later pass of adoption; unconfirmed copies remain pending"
            );
        }
        passes.send_modify(|count| *count += 1);
        tokio::select! {
            _ = tokio::time::sleep(RETRY_INTERVAL) => {}
            changed = peers.changed() => {
                if changed.is_err() {
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        }
        // Notifications coalesce during this delay; passes never overlap.
        tokio::time::sleep_until(next_pass).await;
    }
}

/// Whether every node of `pool` adopted its configurations, as `gossip` has
/// it: this node by what it announced, every other by its latest metadata,
/// and only while gossip holds it alive with protocol 6 or newer. A node of
/// an older release keeps configurations on single nodes and announces
/// nothing; a node that is down may not have adopted its own yet.
pub fn pool_adopted(pool: &[NodeId], local: NodeId, gossip: &GossipHandle) -> bool {
    pool.iter().all(|node| {
        if *node == local {
            return gossip.bucket_configs_adopted(node);
        }
        gossip.get_node(node).is_some_and(|state| {
            state.status == NodeStatus::Alive
                && state.meta.protocol_version >= BUCKET_CONFIG_MIN_PROTOCOL_VERSION
        }) && gossip.bucket_configs_adopted(node)
    })
}

/// The gate at startup: local fallback is available only during initial
/// legacy adoption. Earlier adoption or uncertain configuration history
/// requires quorum reads even before the whole pool's cutover is recorded.
pub async fn initial_gate(index_db: &IndexDb) -> ConfigReadGate {
    match (
        index_db.bucket_config_quorum_reads().await,
        index_db.bucket_config_history_requires_confirmation().await,
    ) {
        (Ok(false), Ok(false)) => ConfigReadGate::default(),
        (Ok(_), Ok(_)) => {
            info!("Bucket configurations require quorum reads: local copies cannot override their owners");
            ConfigReadGate::opened()
        }
        (Err(e), _) | (_, Err(e)) => {
            warn!(error = %e, "could not read configuration history; requiring quorum reads");
            ConfigReadGate::opened()
        }
    }
}

/// Opens `gate` once every node of `pool` adopted its configurations,
/// looking every [`GATE_INTERVAL`], and records that for the next start.
/// Returns at once for a gate already open. Stops early when `shutdown`
/// turns true.
pub async fn open_gate(
    pool: Vec<NodeId>,
    local: NodeId,
    gossip: Arc<GossipHandle>,
    gate: ConfigReadGate,
    index_db: Arc<IndexDb>,
    mut shutdown: watch::Receiver<bool>,
) {
    if gate.is_open() {
        return;
    }
    loop {
        if pool_adopted(&pool, local, &gossip) {
            gate.open();
            if let Err(e) = index_db.mark_bucket_config_quorum_reads().await {
                warn!(
                    error = %e,
                    "could not record that the pool adopted its bucket configurations; the next \
                     start waits for every node of the pool again"
                );
            }
            info!(
                "Every node of the pool adopted its bucket configurations: they are read through \
                 the quorums"
            );
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(GATE_INTERVAL) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}
