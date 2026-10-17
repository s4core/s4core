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

//! How IAM stands on a cluster node, for `GET /api/admin/iam/status` (plan
//! CLUSTER-IAM-REPLICATION.md, `CIR-07-4`): whether the node checks IAM
//! credentials now and why not, how every node of the pool stands with its
//! copy, who holds the lease of IAM changes as far as this node knows, and
//! whether IAM changes are open.
//!
//! Everything comes from what the node holds already — its copy, gossip, its
//! own lease ledger, its gates — so asking costs no call to any peer, as
//! everything a node does at rest.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::cluster_metrics::PoolRoster;
use crate::hlc::HlcTimestamp;
use crate::iam_adoption::IamAdoptionGate;
use crate::iam_catch_up::{IamCatchUp, IamFreshness, IamPoolView, IamRefusal};
use crate::iam_lease::{IamLeaseKeeper, LiveGrant};
use crate::identity::NodeId;

/// An IAM epoch as the status shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EpochView {
    /// The wall time of the newest record, in milliseconds since the Unix
    /// epoch; zero for a copy without records.
    pub wall_ms: u64,
    /// The whole HLC.
    pub hlc: String,
}

impl From<HlcTimestamp> for EpochView {
    fn from(hlc: HlcTimestamp) -> Self {
        Self {
            wall_ms: hlc.wall_time,
            hlc: hlc.to_string(),
        }
    }
}

/// Whether this node checks IAM credentials now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CredentialsView {
    /// `serving`; `starting` until a majority of the pool first agreed with
    /// the copy; `stale` once none has for longer than the bound.
    pub state: &'static str,
    /// How long ago a majority of the pool last agreed with the copy, in
    /// milliseconds; `None` before the first time.
    pub confirmed_ago_ms: Option<u64>,
    /// `S4_IAM_MAX_STALENESS_SECS`; `None` when the bound is off.
    pub max_staleness_secs: Option<u64>,
}

/// One node of the pool as this node sees its copy of IAM.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeView {
    /// Its name in `S4_POOL_NODES`.
    pub node: String,
    /// Its identity.
    pub node_id: String,
    /// Whether it is the node answering.
    pub local: bool,
    /// Its liveness as gossip holds it: `alive`, `suspect`, `dead`, `left`,
    /// or `unknown`.
    pub status: &'static str,
    /// The epoch it announces; `None` before it announced one.
    pub epoch: Option<EpochView>,
    /// How far its newest record is behind the newest any node of the pool
    /// announces, in milliseconds. A delete of a record a node never held
    /// leaves it behind for good without its copy missing anything that
    /// matters, so `agrees` is what counts.
    pub epoch_behind_ms: Option<u64>,
    /// Whether it agrees with this node's copy: alive and holding no version
    /// this node lacks.
    pub agrees: bool,
}

/// The lease of IAM changes as this node knows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LeaseView {
    /// The highest epoch this node granted or learned of.
    pub max_epoch: u64,
    /// The grant this node holds out now, when a change of IAM is under way.
    pub held: Option<HeldView>,
}

/// A grant this node holds out now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeldView {
    /// Its epoch.
    pub epoch: u64,
    /// The node holding it.
    pub holder: String,
    /// How much longer it runs by this node's clock, in milliseconds.
    pub remaining_ms: u64,
}

/// How IAM stands on a cluster node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IamStatus {
    /// The node answering.
    pub node: String,
    /// Whether it checks IAM credentials now.
    pub credentials: CredentialsView,
    /// Its copy's epoch; `None` until it read its copy.
    pub epoch: Option<EpochView>,
    /// How many nodes, it included, agree with its copy now.
    pub agreeing: usize,
    /// How many confirm the copy.
    pub majority: usize,
    /// Every node of the pool.
    pub nodes: Vec<NodeView>,
    /// The lease of IAM changes.
    pub lease: LeaseView,
    /// `adopted` once IAM changes are open on this node, `pending` while it
    /// waits for the pool to take over IAM its nodes kept of their own.
    pub adoption: &'static str,
}

/// Reads [`IamStatus`] off the parts of a cluster node that keep IAM.
pub struct IamStatusSource {
    local: NodeId,
    roster: PoolRoster,
    catch_up: Arc<IamCatchUp>,
    freshness: Arc<IamFreshness>,
    keeper: Arc<IamLeaseKeeper>,
    gate: IamAdoptionGate,
}

impl IamStatusSource {
    /// The status of node `local`, its pool named by `roster`.
    pub fn new(
        local: NodeId,
        roster: PoolRoster,
        catch_up: Arc<IamCatchUp>,
        freshness: Arc<IamFreshness>,
        keeper: Arc<IamLeaseKeeper>,
        gate: IamAdoptionGate,
    ) -> Self {
        Self {
            local,
            roster,
            catch_up,
            freshness,
            keeper,
            gate,
        }
    }

    /// How IAM stands on this node now.
    pub async fn status(&self) -> IamStatus {
        let max_epoch = self.keeper.max_epoch().await;
        let live = self.keeper.live_grant().await;
        assemble(StatusParts {
            local: self.local,
            names: &|node| {
                self.roster.name_of(node).map_or_else(|| node.to_string(), str::to_string)
            },
            view: self.catch_up.pool_view(),
            refusal: self.freshness.check().err(),
            since_confirmed: self.freshness.since_confirmed(),
            max_staleness: self.freshness.max_staleness(),
            max_epoch,
            live,
            adopted: self.gate.is_open(),
        })
    }
}

/// What the status is assembled from.
struct StatusParts<'a> {
    local: NodeId,
    names: &'a dyn Fn(NodeId) -> String,
    view: Option<IamPoolView>,
    refusal: Option<IamRefusal>,
    since_confirmed: Option<Duration>,
    max_staleness: Option<Duration>,
    max_epoch: u64,
    live: Option<LiveGrant>,
    adopted: bool,
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn status_name(status: Option<crate::identity::NodeStatus>) -> &'static str {
    status.map_or("unknown", |status| status.as_str())
}

fn assemble(parts: StatusParts<'_>) -> IamStatus {
    let names = parts.names;
    let newest = parts
        .view
        .iter()
        .flat_map(|view| view.nodes.iter())
        .filter_map(|node| node.announced)
        .map(|announced| announced.epoch.wall_time)
        .max();
    let nodes = parts
        .view
        .iter()
        .flat_map(|view| view.nodes.iter())
        .map(|node| NodeView {
            node: names(node.node),
            node_id: node.node.to_string(),
            local: node.local,
            status: status_name(node.status),
            epoch: node.announced.map(|announced| announced.epoch.into()),
            epoch_behind_ms: node
                .announced
                .zip(newest)
                .map(|(announced, newest)| newest.saturating_sub(announced.epoch.wall_time)),
            agrees: node.agrees,
        })
        .collect::<Vec<_>>();
    IamStatus {
        node: names(parts.local),
        credentials: CredentialsView {
            state: parts.refusal.map_or("serving", IamRefusal::label),
            confirmed_ago_ms: parts.since_confirmed.map(millis),
            max_staleness_secs: parts.max_staleness.map(|bound| bound.as_secs()),
        },
        epoch: parts.view.as_ref().map(|view| view.ours.epoch.into()),
        agreeing: nodes.iter().filter(|node| node.agrees).count(),
        majority: parts.view.as_ref().map_or(0, |view| view.majority),
        nodes,
        lease: LeaseView {
            max_epoch: parts.max_epoch,
            held: parts.live.map(|live| HeldView {
                epoch: live.epoch,
                holder: names(live.holder),
                remaining_ms: live.remaining_ms,
            }),
        },
        adoption: if parts.adopted { "adopted" } else { "pending" },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iam_catch_up::{IamAnnouncement, IamNodeView};
    use crate::identity::NodeStatus;

    fn name(node: NodeId) -> String {
        format!("node-{}", node.0)
    }

    fn announced(wall: u64, digest: u8) -> Option<IamAnnouncement> {
        Some(IamAnnouncement {
            epoch: HlcTimestamp::from_record(wall, 0, 1),
            digest: [digest; 16],
        })
    }

    fn parts(view: Option<IamPoolView>) -> StatusParts<'static> {
        StatusParts {
            local: NodeId(1),
            names: &name,
            view,
            refusal: None,
            since_confirmed: Some(Duration::from_millis(120)),
            max_staleness: Some(Duration::from_secs(30)),
            max_epoch: 7,
            live: None,
            adopted: true,
        }
    }

    fn pool() -> IamPoolView {
        IamPoolView {
            ours: announced(1_000, 1).unwrap(),
            majority: 2,
            nodes: vec![
                IamNodeView {
                    node: NodeId(1),
                    local: true,
                    status: Some(NodeStatus::Alive),
                    announced: announced(1_000, 1),
                    agrees: true,
                },
                IamNodeView {
                    node: NodeId(2),
                    local: false,
                    status: Some(NodeStatus::Alive),
                    announced: announced(1_000, 1),
                    agrees: true,
                },
                IamNodeView {
                    node: NodeId(3),
                    local: false,
                    status: Some(NodeStatus::Dead),
                    announced: announced(400, 2),
                    agrees: false,
                },
            ],
        }
    }

    #[test]
    fn every_node_with_its_agreement_and_how_far_behind_it_is() {
        let status = assemble(parts(Some(pool())));
        assert_eq!(status.node, "node-1");
        assert_eq!(status.credentials.state, "serving");
        assert_eq!(status.credentials.confirmed_ago_ms, Some(120));
        assert_eq!(status.credentials.max_staleness_secs, Some(30));
        assert_eq!((status.agreeing, status.majority), (2, 2));
        let nodes: Vec<_> = status
            .nodes
            .iter()
            .map(|node| {
                (
                    node.node.as_str(),
                    node.status,
                    node.agrees,
                    node.epoch_behind_ms,
                )
            })
            .collect();
        assert_eq!(
            nodes,
            vec![
                ("node-1", "alive", true, Some(0)),
                ("node-2", "alive", true, Some(0)),
                ("node-3", "dead", false, Some(600)),
            ]
        );
        assert_eq!(status.epoch.unwrap().wall_ms, 1_000);
        assert_eq!(status.adoption, "adopted");
        assert_eq!(status.lease.held, None);
    }

    #[test]
    fn a_refusal_the_lease_held_and_adoption_pending_are_named() {
        let mut stale = parts(Some(pool()));
        stale.refusal = Some(IamRefusal::Stale);
        stale.live = Some(LiveGrant {
            epoch: 8,
            holder: NodeId(2),
            remaining_ms: 3_000,
        });
        stale.adopted = false;
        let status = assemble(stale);
        assert_eq!(status.credentials.state, "stale");
        assert_eq!(
            status.lease.held,
            Some(HeldView {
                epoch: 8,
                holder: "node-2".into(),
                remaining_ms: 3_000
            })
        );
        assert_eq!(status.lease.max_epoch, 7);
        assert_eq!(status.adoption, "pending");
    }

    #[test]
    fn before_the_copy_is_read_nothing_is_made_up() {
        let mut starting = parts(None);
        starting.refusal = Some(IamRefusal::Starting);
        starting.since_confirmed = None;
        starting.max_staleness = None;
        let status = assemble(starting);
        assert_eq!(status.credentials.state, "starting");
        assert_eq!(status.credentials.confirmed_ago_ms, None);
        assert_eq!(status.credentials.max_staleness_secs, None);
        assert_eq!(status.epoch, None);
        assert!(status.nodes.is_empty());
        assert_eq!((status.agreeing, status.majority), (0, 0));
    }
}
