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

//! A node's copy of IAM kept current with its pool, and how current it is
//! (plan CLUSTER-IAM-REPLICATION.md, decision 4).
//!
//! Every node checks signatures and tokens against its own copy of the
//! cluster's control state, without asking any other node. That is safe only
//! while the copy holds every change the pool confirmed: a change is
//! confirmed once a majority holds it, so a copy that a majority of the pool
//! has agreed with — each of them holding no version this one lacks — holds
//! every change confirmed before that agreement.
//!
//! - [`IamCopy`] is the node's copy as the pool compares it: the fingerprint
//!   of its live records and its epoch, the HLC of its newest record,
//!   deletes included. Every node announces both in gossip
//!   ([`IamAnnouncement`]).
//! - [`IamCatchUp`] compares the copy with what every live peer announces. A
//!   peer that announces the same fingerprint agrees with it; one that
//!   announces another is asked for its versions (`IamVersions`), and every
//!   key it holds in a newer version is settled the way anti-entropy settles
//!   a pair of nodes — at once, not at the next round. A peer whose
//!   versions this node holds or outranks agrees with it as well, and a
//!   peer that has moved on since vouches for the copy as of the moment the
//!   catch-up began: the copy holds every version the peer held then. So a
//!   pool that changes IAM without pause, whose copies seldom agree at the
//!   moment they are compared, still confirms them once a catch-up. Nothing
//!   is asked of any peer while the copies agree: gossip carries the
//!   comparison.
//! - [`IamFreshness`] is the gate: until a majority of the pool, this node
//!   included, has agreed with the copy since the node started, IAM
//!   credentials are refused as `starting`; once no majority has agreed with
//!   it for longer than `S4_IAM_MAX_STALENESS_SECS`, as `stale` — the node is
//!   in the minority of a split, and a client is to retry through another
//!   node rather than take its key for a wrong one.

mod catch_up;
mod copy;
mod freshness;

pub use catch_up::{IamCatchUp, IamCatchUpConfig, IamNodeView, IamPoolView};
pub use copy::{ClassVersion, IamCopy, VersionsPage};
pub use freshness::{IamFreshness, IamRefusal};

use serde::{Deserialize, Serialize};

use crate::hlc::HlcTimestamp;

/// What a node announces in gossip of its copy of IAM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IamAnnouncement {
    /// The node's IAM epoch: the HLC of the newest record of the cluster's
    /// control state it holds, deletes included; zero for none.
    pub epoch: HlcTimestamp,
    /// The fingerprint of its live records: two nodes with the same one hold
    /// the same version of every key. A deleted key and a key never written
    /// are both absent and do not tell two copies apart.
    pub digest: [u8; 16],
}
