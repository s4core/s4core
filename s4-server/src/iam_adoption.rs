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

//! A cluster node's part in its pool taking over the IAM nodes kept of their
//! own (plan CLUSTER-IAM-REPLICATION.md, decision 8).
//!
//! Every [`TICK`] the [`IamAdoptionWorker`]:
//!
//! 1. drops the IAM this node keeps of its own once the pool's decision covers
//!    it — the decision names the fingerprint of what the node answered, and
//!    the users it chose are in the pool already;
//! 2. announces in gossip whether it keeps any;
//! 3. opens IAM changes on this node once every node of the pool announced it
//!    keeps none, and records that, so the next start opens them at once
//!    ([`initial_gate`]). A pool where no node ever kept IAM of its own opens
//!    by itself: there is nothing to decide.
//!
//! None of it sends a request to a peer: the decision arrives as any record
//! of the cluster's control state, and the announcements with the metadata
//! gossip spreads anyway.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use s4_cluster::{iam_pool_adopted, GossipHandle, IamAdoptionGate, NodeId};
use s4_core::storage::IndexDb;
use s4_features::iam::{IamStorage, UserRecords};
use tokio::sync::watch;
use tracing::{info, warn};

/// How often the worker looks.
pub const TICK: Duration = Duration::from_secs(1);

/// The gate as this node starts with it: open when an earlier start saw every
/// node of the pool keep no IAM of its own, closed otherwise. A pool that
/// adopted its IAM gets no more of it on single nodes — every change goes
/// through the pool — so a node down now does not reopen the question.
pub async fn initial_gate(index_db: &IndexDb) -> IamAdoptionGate {
    match index_db.iam_adopted().await {
        Ok(true) => {
            info!("The pool adopted its IAM: IAM changes go through the pool");
            IamAdoptionGate::opened()
        }
        Ok(false) => IamAdoptionGate::closed(),
        Err(e) => {
            warn!(error = %e, "could not read whether the pool adopted its IAM");
            IamAdoptionGate::closed()
        }
    }
}

/// This node's part in the adoption of its pool's IAM.
pub struct IamAdoptionWorker {
    iam: Arc<IamStorage>,
    root_name: Option<String>,
    local: NodeId,
    pool: Vec<NodeId>,
    gossip: Arc<GossipHandle>,
    gate: IamAdoptionGate,
    index_db: Option<Arc<IndexDb>>,
    /// The records of an earlier release a node serving none of them left
    /// over — a stored root without keys — were dropped.
    leftovers_dropped: AtomicBool,
    /// A decision that does not cover what this node keeps was reported.
    uncovered_reported: AtomicBool,
}

impl IamAdoptionWorker {
    /// The worker of node `local` of `pool`, over its IAM `iam` with root
    /// `root_name`, announcing through `gossip`, opening `gate` and
    /// recording it in `index_db`.
    pub fn new(
        iam: Arc<IamStorage>,
        root_name: Option<String>,
        local: NodeId,
        pool: Vec<NodeId>,
        gossip: Arc<GossipHandle>,
        gate: IamAdoptionGate,
        index_db: Option<Arc<IndexDb>>,
    ) -> Self {
        Self {
            iam,
            root_name,
            local,
            pool,
            gossip,
            gate,
            index_db,
            leftovers_dropped: AtomicBool::new(false),
            uncovered_reported: AtomicBool::new(false),
        }
    }

    /// One look: drop what the decision covers, announce, open the gate.
    pub async fn tick(&self) {
        if self.iam.directory().records() == UserRecords::Legacy {
            self.drop_when_covered().await;
        }
        let adopted = self.iam.directory().records() == UserRecords::Current;
        self.gossip.announce_iam_adopted(adopted);
        if !self.gate.is_open() && iam_pool_adopted(&self.pool, self.local, &self.gossip) {
            self.gate.open();
            if let Some(index_db) = &self.index_db {
                if let Err(e) = index_db.mark_iam_adopted().await {
                    warn!(
                        error = %e,
                        "could not record that the pool adopted its IAM; the next start waits \
                         for every node of the pool again"
                    );
                }
            }
            info!(
                "Every node of the pool keeps no IAM users of its own: IAM changes go through \
                 the pool"
            );
        }
        // A node that serves the pool's users drops what an earlier release
        // left beside them, a stored root without keys; one that still serves
        // users of its own keeps them whatever the gate says.
        if self.gate.is_open() && adopted && !self.leftovers_dropped.load(Ordering::Acquire) {
            match self.iam.drop_legacy().await {
                Ok(0) => {}
                Ok(dropped) => info!(
                    dropped,
                    "Records of an earlier release no user needs any more removed"
                ),
                Err(e) => {
                    warn!(error = %e, "could not remove the records of an earlier release");
                    return;
                }
            }
            self.leftovers_dropped.store(true, Ordering::Release);
        }
    }

    /// Drops what this node keeps of its own once the pool's decision covers
    /// exactly that.
    async fn drop_when_covered(&self) {
        let (own, decision) = match (
            self.iam.own_users(self.root_name.as_deref()).await,
            self.iam.adoption_record().await,
        ) {
            (Ok(own), Ok(decision)) => (own, decision),
            (Err(e), _) | (_, Err(e)) => {
                warn!(error = %e, "could not read this node's own IAM or the pool's decision");
                return;
            }
        };
        let Some(decision) = decision else {
            return;
        };
        if !decision.covers(&self.local.to_string(), own.digest()) {
            if !self.uncovered_reported.swap(true, Ordering::AcqRel) {
                warn!(
                    report_id = %decision.report_id,
                    "The pool's decision on IAM does not cover the users this node keeps of its \
                     own: they stay until a decision on a report that names them"
                );
            }
            return;
        }
        match self.iam.drop_legacy().await {
            Ok(dropped) => info!(
                dropped,
                report_id = %decision.report_id,
                "The pool took over the IAM users this node kept of its own: it serves the \
                 pool's users from now on"
            ),
            Err(e) => warn!(error = %e, "could not drop the IAM this node kept of its own"),
        }
    }

    /// Looks every [`TICK`] until `shutdown` turns true.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            self.tick().await;
            tokio::select! {
                _ = tokio::time::sleep(TICK) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }
}
