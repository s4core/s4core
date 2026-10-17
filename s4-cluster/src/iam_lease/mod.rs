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

//! The lease of IAM changes, with fencing (plan CLUSTER-IAM-REPLICATION.md,
//! decision 3).
//!
//! Last writer wins is wrong for accounts: a new password through one node
//! and a new role through another lose one of two confirmed changes, and two
//! creations of one name make two users. So every change of IAM is made
//! under a lease: the node that took the request asks every node of the pool
//! for the next epoch ([`IamLeaseHolder`]), holds the lease once a majority
//! granted it, reads what it changes from a majority, checks its
//! preconditions, writes to a majority and releases.
//!
//! Each node keeps its grants ([`IamLeaseKeeper`], on a [`LeaseLedger`]) and
//! fences the writes of the cluster's control state: a write of an epoch
//! behind the node's, of a grant that ran out on it, or with an HLC outside
//! the window of its epoch, is refused. The window — above every HLC an
//! earlier epoch may have written with, below the epoch's own ceiling — makes
//! every write of a later epoch outrank every write of an earlier one, so a
//! holder that woke up from a pause after its lease passed on cannot have a
//! write of its own win anywhere.
//!
//! The lease is not a leader: it is held by the node a request came to, for
//! one change. Its epoch would become the term of a consensus log of the
//! cluster's metadata, with the same fencing on the replicas.

pub(crate) mod holder;
mod keeper;
mod ledger;

#[cfg(test)]
mod model_tests;

pub use holder::{IamLeaseConfig, IamLeaseError, IamLeaseHolder, IamMutationLease, LeaseRead};
pub use keeper::{Admitted, ClassRecord, IamLeaseKeeper, Refusal, LEASE_RECORD_KEY};
pub use ledger::{EpochRecord, Fenced, Grant, LeaseFence, LeaseLedger, LedgerRecord, LiveGrant};

use crate::hlc::HlcTimestamp;
use crate::identity::NodeId;
use crate::rpc::proto;

/// The HLC of an optional field of the protocol; zero when absent.
pub(crate) fn fence_from_proto_hlc(hlc: Option<&proto::HybridTimestamp>) -> HlcTimestamp {
    hlc.map(HlcTimestamp::from_proto).unwrap_or_else(HlcTimestamp::zero)
}

/// A node's grant as it goes over the wire.
pub(crate) fn grant_to_proto(grant: Grant, max_epoch: u64) -> proto::IamLeaseAcquireResponse {
    match grant {
        Grant::Granted { floor } => proto::IamLeaseAcquireResponse {
            granted: true,
            floor: Some(floor.to_proto()),
            max_epoch,
            ..Default::default()
        },
        Grant::Busy { remaining_ms, .. } => proto::IamLeaseAcquireResponse {
            busy: true,
            busy_remaining_ms: remaining_ms,
            max_epoch,
            ..Default::default()
        },
        Grant::Stale => proto::IamLeaseAcquireResponse {
            max_epoch,
            ..Default::default()
        },
    }
}

impl LeaseFence {
    /// As it goes with a write or a delete.
    pub fn to_proto(self) -> proto::IamLeaseFence {
        proto::IamLeaseFence {
            epoch: self.epoch,
            holder: self.holder.to_bytes().to_vec(),
            floor: Some(self.floor.to_proto()),
            ceiling: Some(self.ceiling.to_proto()),
        }
    }

    /// As it came with a write or a delete; `None` when it names no holder a
    /// node could be.
    pub fn from_proto(fence: &proto::IamLeaseFence) -> Option<Self> {
        Some(Self {
            epoch: fence.epoch,
            holder: NodeId::from_bytes(&fence.holder).ok()?,
            floor: fence_from_proto_hlc(fence.floor.as_ref()),
            ceiling: fence_from_proto_hlc(fence.ceiling.as_ref()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fence_goes_over_the_wire_unchanged() {
        let fence = LeaseFence {
            epoch: 12,
            holder: NodeId(0xfeed),
            floor: HlcTimestamp::from_record(1_000, 2, 3),
            ceiling: HlcTimestamp::from_record(6_000, 0, 0),
        };
        assert_eq!(LeaseFence::from_proto(&fence.to_proto()), Some(fence));
        let mut no_holder = fence.to_proto();
        no_holder.holder.clear();
        assert_eq!(LeaseFence::from_proto(&no_holder), None);
    }
}
