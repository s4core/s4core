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

//! IAM changes of a cluster node, through the quorums of the pool.
//!
//! The IAM records of version 2 are the cluster's control state
//! (`ReplicationClass::ClusterControl`): every node of the pool holds them,
//! and a write or a read takes a majority of the pool (plan
//! CLUSTER-IAM-REPLICATION.md, decision 1). Every change is made under the
//! lease of IAM changes (decision 3): the node takes it from a majority, reads
//! what it changes from a majority under the lease's epoch — every node that
//! answers refuses the writes of earlier epochs from then on — checks its
//! preconditions, writes to a majority fenced by the epoch, and releases it.
//! A node that missed a write gets a hint, and anti-entropy compares the
//! records between any two nodes of the pool. A delete leaves a tombstone, so
//! a deleted user does not come back from a node that missed the delete.

use std::collections::HashMap;
use std::sync::Arc;

use s4_cluster::iam_lease::ClassRecord;
use s4_cluster::{
    ClusterError, IamAdoptionCollector, IamAdoptionGate, IamFreshness, IamLeaseError,
    IamLeaseHolder, IamMutationLease, IamRefusal, QuorumWriteCoordinator,
};
use s4_core::types::SYSTEM_BUCKET;
use s4_features::iam::{
    AdoptionGate, CredentialGate, IamChange, IamError, IamRecordStore, NodeHoldings, NotServing,
    OwnUsers,
};

/// Checks IAM credentials only while a majority of the pool agrees with this
/// node's copy of IAM (decision 4): closed until it first has since the node
/// started, and once it has not for longer than the staleness bound.
pub struct ClusterCredentialGate {
    freshness: Arc<IamFreshness>,
}

impl ClusterCredentialGate {
    /// The gate `freshness` keeps.
    pub fn new(freshness: Arc<IamFreshness>) -> Self {
        Self { freshness }
    }
}

impl CredentialGate for ClusterCredentialGate {
    fn admit(&self) -> Result<(), NotServing> {
        self.freshness.admit().map_err(|refusal| match refusal {
            IamRefusal::Starting => NotServing::Starting,
            IamRefusal::Stale => NotServing::Stale,
        })
    }
}

/// Opens IAM changes once every node of the pool announced that it keeps no
/// IAM of its own the pool has not taken over (decision 8).
pub struct ClusterAdoptionGate {
    gate: IamAdoptionGate,
}

impl ClusterAdoptionGate {
    /// The gate `gate` keeps.
    pub fn new(gate: IamAdoptionGate) -> Self {
        Self { gate }
    }
}

impl AdoptionGate for ClusterAdoptionGate {
    fn is_open(&self) -> bool {
        self.gate.is_open()
    }
}

/// The pool taking over the IAM its nodes kept of their own, as a cluster
/// node's admin API reaches it (decision 8): every node's answer, and the
/// gate those answers open.
pub struct ClusterIamAdoption {
    collector: Arc<IamAdoptionCollector>,
    gate: IamAdoptionGate,
}

impl ClusterIamAdoption {
    /// The adoption `collector` asks the pool for, opening `gate`.
    pub fn new(collector: Arc<IamAdoptionCollector>, gate: IamAdoptionGate) -> Self {
        Self { collector, gate }
    }

    /// Whether IAM changes are open on this node.
    pub fn gate(&self) -> &IamAdoptionGate {
        &self.gate
    }

    /// This node's name in the pool.
    pub fn local_name(&self) -> String {
        self.collector.name_of(self.collector.local())
    }

    /// What every node of the pool keeps of its own, a stored root without S3
    /// credentials left out: root is `root_name`, from the environment.
    pub async fn holdings(&self, root_name: Option<&str>) -> Vec<NodeHoldings> {
        self.collector
            .collect()
            .await
            .into_iter()
            .map(|answer| NodeHoldings {
                node_id: answer.node.to_string(),
                node_name: answer.name,
                own: answer.records.and_then(|records| {
                    OwnUsers::from_records(records, root_name).map_err(|e| e.to_string())
                }),
            })
            .collect()
    }
}

/// Changes the IAM records under the lease, through the pool's quorums.
pub struct ClusterRecordStore {
    write: Arc<QuorumWriteCoordinator>,
    lease: Arc<IamLeaseHolder>,
}

impl ClusterRecordStore {
    /// Records changed through `write`, under leases `lease` takes.
    pub fn new(write: Arc<QuorumWriteCoordinator>, lease: Arc<IamLeaseHolder>) -> Self {
        Self { write, lease }
    }
}

/// A failure of the pool as IAM reports it: too few nodes answering is a
/// retry for the caller, anything else a fault.
fn iam_error(error: ClusterError) -> IamError {
    match error {
        ClusterError::QuorumNotMet { .. }
        | ClusterError::WriteTimeout(_)
        | ClusterError::ReadTimeout(_)
        | ClusterError::EpochMismatch { .. }
        | ClusterError::Unavailable(_) => IamError::Unavailable(error.to_string()),
        other => IamError::Cluster(other.to_string()),
    }
}

/// The lease's refusal as IAM reports it.
fn lease_error(error: IamLeaseError) -> IamError {
    match error {
        IamLeaseError::Busy { retry_after } => IamError::Busy {
            retry_after_secs: retry_after.as_secs().max(1),
        },
        IamLeaseError::Unavailable(reason) => IamError::Unavailable(reason),
    }
}

#[async_trait::async_trait]
impl IamRecordStore for ClusterRecordStore {
    async fn begin(&self) -> Result<Box<dyn IamChange>, IamError> {
        let lease = self.lease.acquire().await.map_err(lease_error)?;
        Ok(Box::new(ClusterChange {
            write: self.write.clone(),
            holder: self.lease.clone(),
            lease,
        }))
    }
}

/// One change of IAM under a lease this node holds.
struct ClusterChange {
    write: Arc<QuorumWriteCoordinator>,
    holder: Arc<IamLeaseHolder>,
    lease: IamMutationLease,
}

impl ClusterChange {
    /// The records under `prefix` as a majority holds them, read under the
    /// lease, each held by a majority before the change acts on it: a
    /// version only a few nodes hold — a change that failed, a write or a
    /// delete — is written again under this lease. Another read could miss
    /// it, and a change that relied on it — a name freed by a delete — would
    /// then stand beside the version it replaced.
    async fn settled(&self, prefix: &str) -> Result<Vec<ClassRecord>, IamError> {
        let read = self.holder.read(&self.lease, prefix).await.map_err(lease_error)?;
        for record in read.records.iter().filter(|record| read.unsettled.contains(&record.key)) {
            match &record.data {
                Some(data) => self.write(&record.key, data).await?,
                None => self.delete(&record.key).await?,
            }
        }
        Ok(read.records)
    }

    /// Renews the lease if the change has run long, before it writes.
    async fn ensure_time(&self) -> Result<(), IamError> {
        self.holder.ensure_time(&self.lease).await.map_err(lease_error)
    }
}

#[async_trait::async_trait]
impl IamChange for ClusterChange {
    fn epoch(&self) -> u64 {
        self.lease.fence().epoch
    }

    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, IamError> {
        let records = self.settled(key).await?;
        Ok(records
            .into_iter()
            .find(|record| record.key == key)
            .and_then(|record| record.data))
    }

    async fn records(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, IamError> {
        let records = self.settled(prefix).await?;
        Ok(records
            .into_iter()
            .filter_map(|record| record.data.map(|data| (record.key, data)))
            .collect())
    }

    async fn write(&self, key: &str, data: &[u8]) -> Result<(), IamError> {
        self.ensure_time().await?;
        crate::iam_test_hooks::stall_before_write(key, data).await;
        self.write
            .write_fenced(
                SYSTEM_BUCKET,
                key,
                data,
                "application/json",
                &HashMap::new(),
                self.lease.fence(),
            )
            .await
            .map(|_| ())
            .map_err(iam_error)
    }

    async fn delete(&self, key: &str) -> Result<(), IamError> {
        self.ensure_time().await?;
        match self.write.delete_fenced(SYSTEM_BUCKET, key, self.lease.fence()).await {
            Ok(_) | Err(ClusterError::ObjectNotFound { .. }) => Ok(()),
            Err(e) => Err(iam_error(e)),
        }
    }

    async fn finish(self: Box<Self>) {
        let Self { holder, lease, .. } = *self;
        holder.release(lease).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn too_few_nodes_is_unavailable_and_anything_else_a_fault() {
        for error in [
            ClusterError::QuorumNotMet {
                needed: 2,
                achieved: 1,
            },
            ClusterError::WriteTimeout(std::time::Duration::from_secs(1)),
            ClusterError::ReadTimeout(std::time::Duration::from_secs(1)),
            ClusterError::EpochMismatch {
                coordinator: 1,
                replica: 2,
            },
            ClusterError::Unavailable("down".into()),
        ] {
            assert!(matches!(iam_error(error), IamError::Unavailable(_)));
        }
        assert!(matches!(
            iam_error(ClusterError::Storage("disk".into())),
            IamError::Cluster(_)
        ));
    }

    #[test]
    fn the_gate_is_the_freshness_of_the_copy() {
        let freshness = Arc::new(IamFreshness::new(std::time::Duration::from_secs(30)));
        let gate = ClusterCredentialGate::new(freshness.clone());
        assert_eq!(gate.admit(), Err(NotServing::Starting));
        freshness.confirm(std::time::Instant::now());
        assert_eq!(gate.admit(), Ok(()));

        let stale = Arc::new(IamFreshness::new(std::time::Duration::from_millis(10)));
        stale.confirm(std::time::Instant::now());
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert_eq!(
            ClusterCredentialGate::new(stale).admit(),
            Err(NotServing::Stale)
        );
    }

    #[test]
    fn a_busy_lease_is_a_retry_after_at_least_a_second() {
        assert!(matches!(
            lease_error(IamLeaseError::Busy {
                retry_after: std::time::Duration::from_millis(300)
            }),
            IamError::Busy {
                retry_after_secs: 1
            }
        ));
        assert!(matches!(
            lease_error(IamLeaseError::Unavailable("one of three".into())),
            IamError::Unavailable(_)
        ));
    }
}
