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

//! The pool taking over the IAM its nodes kept of their own (plan
//! CLUSTER-IAM-REPLICATION.md, decision 8), as the cluster carries it.
//!
//! - [`IamOwnUsers`] answers `IamOwnUsers`: the user records an earlier
//!   release, or the node as a single node, stored on this node alone.
//! - [`IamAdoptionCollector`] asks every node of the pool for them, this one
//!   included, for the report the operator decides on.
//! - [`IamAdoptionGate`] opens IAM changes on this node once every node of
//!   the pool announced in gossip that it keeps nothing of its own
//!   ([`pool_adopted`]).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use s4_core::types::{IAM_LEGACY_USER_KEY_PREFIX, SYSTEM_BUCKET};
use s4_core::StorageEngine;

use crate::cluster_metrics::PoolRoster;
use crate::error::ClusterError;
use crate::gossip::GossipHandle;
use crate::identity::{NodeId, NodeStatus};
use crate::protocol::IAM_REPLICATION_MIN_PROTOCOL_VERSION;
use crate::rpc::{proto, NodeClient};

/// Records a node answers in one page of `IamOwnUsers`.
const PAGE: u32 = 500;

/// How long the collector waits for one node's answer.
const NODE_TIMEOUT: Duration = Duration::from_secs(10);

/// Whether IAM may be changed through this node: closed until every node of
/// the pool announced that it keeps no IAM of its own the pool has not taken
/// over. Once open it stays open.
#[derive(Debug, Clone, Default)]
pub struct IamAdoptionGate(Arc<AtomicBool>);

impl IamAdoptionGate {
    /// A gate already open: an earlier start saw the pool adopt its IAM.
    pub fn opened() -> Self {
        let gate = Self::default();
        gate.open();
        gate
    }

    /// A gate closed until the pool adopts its IAM, and says so in
    /// `s4_iam_adoption_pending`.
    pub fn closed() -> Self {
        crate::cluster_metrics::set_iam_adoption_pending(true);
        Self::default()
    }

    /// Whether IAM may be changed through the pool.
    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Opens the gate, and says so in `s4_iam_adoption_pending`.
    pub fn open(&self) {
        self.0.store(true, Ordering::Release);
        crate::cluster_metrics::set_iam_adoption_pending(false);
    }
}

/// Whether every node of `pool` announced that it keeps no IAM of its own,
/// as `gossip` has it: this node by what it announced, every other by its
/// latest metadata, and only while gossip holds it alive with protocol 8 or
/// newer. A node of an earlier release keeps IAM of its own by definition,
/// and a node that is down may.
pub fn pool_adopted(pool: &[NodeId], local: NodeId, gossip: &GossipHandle) -> bool {
    pool.iter().all(|node| {
        if *node == local {
            return gossip.iam_adopted(node);
        }
        gossip.get_node(node).is_some_and(|state| {
            state.status == NodeStatus::Alive
                && state.meta.protocol_version >= IAM_REPLICATION_MIN_PROTOCOL_VERSION
        }) && gossip.iam_adopted(node)
    })
}

/// Seals the secret of S3 credentials in a user record before the record
/// leaves the node (plan CLUSTER-IAM-REPLICATION.md, decision 7).
#[async_trait::async_trait]
pub trait OwnRecordSealer: Send + Sync {
    /// `data`, a user record, with its secret sealed.
    ///
    /// # Errors
    ///
    /// Why it could not be sealed.
    async fn seal(&self, data: &[u8]) -> Result<Vec<u8>, String>;
}

/// The user records this node keeps of its own, read from its storage.
pub struct IamOwnUsers {
    storage: Arc<dyn StorageEngine>,
    sealer: Option<Arc<dyn OwnRecordSealer>>,
}

impl IamOwnUsers {
    /// The records `storage`, this node's own, holds.
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            sealer: None,
        }
    }

    /// The same records, their secrets sealed by `sealer` as they are read.
    pub fn with_sealer(mut self, sealer: Arc<dyn OwnRecordSealer>) -> Self {
        self.sealer = Some(sealer);
        self
    }

    /// Up to `max_keys` records after `start_after`, and whether more follow.
    ///
    /// # Errors
    ///
    /// `Storage` when they cannot be listed or read.
    pub async fn page(
        &self,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Result<(Vec<(String, Vec<u8>)>, bool), ClusterError> {
        let storage_error = |e: s4_core::error::StorageError| ClusterError::Storage(e.to_string());
        let listed = match start_after {
            None => {
                self.storage
                    .list_objects(SYSTEM_BUCKET, IAM_LEGACY_USER_KEY_PREFIX, max_keys + 1)
                    .await
            }
            Some(after) => {
                self.storage
                    .list_objects_after(
                        SYSTEM_BUCKET,
                        IAM_LEGACY_USER_KEY_PREFIX,
                        after,
                        max_keys + 1,
                    )
                    .await
            }
        }
        .map_err(storage_error)?;
        let is_truncated = listed.len() > max_keys;
        let mut records = Vec::new();
        for (key, _) in listed.into_iter().take(max_keys) {
            if !key.starts_with(IAM_LEGACY_USER_KEY_PREFIX) {
                continue;
            }
            match self.storage.get_object(SYSTEM_BUCKET, &key).await {
                Ok((data, _)) => {
                    let data = match &self.sealer {
                        Some(sealer) => sealer.seal(&data).await.map_err(ClusterError::Storage)?,
                        None => data,
                    };
                    records.push((key, data));
                }
                Err(s4_core::error::StorageError::ObjectNotFound { .. }) => {}
                Err(e) => return Err(storage_error(e)),
            }
        }
        Ok((records, is_truncated))
    }

    /// Every record, a page at a time.
    ///
    /// # Errors
    ///
    /// As [`Self::page`].
    pub async fn all(&self) -> Result<Vec<(String, Vec<u8>)>, ClusterError> {
        let mut records = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let (page, more) = self.page(after.as_deref(), PAGE as usize).await?;
            after = page.last().map(|(key, _)| key.clone()).or(after);
            records.extend(page);
            if !more {
                return Ok(records);
            }
        }
    }
}

/// What one node answered of the IAM it keeps of its own.
#[derive(Debug, Clone)]
pub struct NodeOwnRecords {
    /// The node.
    pub node: NodeId,
    /// Its name in `S4_POOL_NODES`, or its identity.
    pub name: String,
    /// Its records — key and body — or why they could not be read.
    pub records: Result<Vec<(String, Vec<u8>)>, String>,
}

/// Asks every node of the pool for the IAM it keeps of its own.
pub struct IamAdoptionCollector {
    local: NodeId,
    roster: PoolRoster,
    pool: Vec<NodeId>,
    own: Arc<IamOwnUsers>,
    rpc: Arc<NodeClient>,
}

impl IamAdoptionCollector {
    /// The collector of node `local` of the pool `pool`, named by `roster`,
    /// reading its own records from `own` and its peers' through `rpc`.
    pub fn new(
        local: NodeId,
        roster: PoolRoster,
        pool: Vec<NodeId>,
        own: Arc<IamOwnUsers>,
        rpc: Arc<NodeClient>,
    ) -> Self {
        Self {
            local,
            roster,
            pool,
            own,
            rpc,
        }
    }

    /// This node.
    pub fn local(&self) -> NodeId {
        self.local
    }

    /// The name of `node`, as the pool names it.
    pub fn name_of(&self, node: NodeId) -> String {
        self.roster.name_of(node).map_or_else(|| node.to_string(), str::to_string)
    }

    /// Every node's answer, in the order of the pool; a node that does not
    /// answer within a few seconds is reported as not answering.
    pub async fn collect(&self) -> Vec<NodeOwnRecords> {
        let answers = self.pool.iter().map(|node| async move {
            let records = if *node == self.local {
                self.own.all().await.map_err(|e| e.to_string())
            } else {
                match tokio::time::timeout(NODE_TIMEOUT, self.ask(*node)).await {
                    Ok(answer) => answer,
                    Err(_) => Err(format!("no answer within {} s", NODE_TIMEOUT.as_secs())),
                }
            };
            NodeOwnRecords {
                node: *node,
                name: self.name_of(*node),
                records,
            }
        });
        futures::future::join_all(answers).await
    }

    /// The records `node` keeps of its own, every page.
    async fn ask(&self, node: NodeId) -> Result<Vec<(String, Vec<u8>)>, String> {
        let mut records = Vec::new();
        let mut start_after = String::new();
        loop {
            let response = self
                .rpc
                .send_iam_own_users(
                    node,
                    proto::IamOwnUsersRequest {
                        start_after: start_after.clone(),
                        max_keys: PAGE,
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            if !response.error.is_empty() {
                return Err(response.error);
            }
            if let Some(last) = response.records.last() {
                start_after = last.key.clone();
            }
            records.extend(response.records.into_iter().map(|record| (record.key, record.data)));
            if !response.is_truncated {
                return Ok(records);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::storage::BitcaskStorageEngine;
    use std::collections::HashMap;

    #[tokio::test]
    async fn the_own_records_come_a_page_at_a_time_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let engine = BitcaskStorageEngine::new(
            dir.path().join("volumes"),
            dir.path().join("metadata"),
            10 * 1024 * 1024,
            4096,
            false,
        )
        .await
        .unwrap();
        let storage: Arc<dyn StorageEngine> = Arc::new(engine);
        for key in [
            "__s4_iam_user_1",
            "__s4_iam_user_2",
            "__s4_iam_user_3",
            "__s4_iam_username_alice",
            "__s4_iam_v2_user_9",
        ] {
            storage
                .put_object(
                    SYSTEM_BUCKET,
                    key,
                    key.as_bytes(),
                    "application/json",
                    &HashMap::new(),
                )
                .await
                .unwrap();
        }
        let own = IamOwnUsers::new(storage);
        let (first, more) = own.page(None, 2).await.unwrap();
        assert!(more);
        assert_eq!(first.len(), 2);
        let (rest, more) = own.page(Some("__s4_iam_user_2"), 2).await.unwrap();
        assert!(!more);
        assert_eq!(
            rest,
            vec![("__s4_iam_user_3".to_string(), b"__s4_iam_user_3".to_vec())]
        );
        let keys: Vec<String> = own.all().await.unwrap().into_iter().map(|(key, _)| key).collect();
        assert_eq!(
            keys,
            ["__s4_iam_user_1", "__s4_iam_user_2", "__s4_iam_user_3"]
        );

        // With a sealer, every record leaves through it.
        struct Upper;
        #[async_trait::async_trait]
        impl OwnRecordSealer for Upper {
            async fn seal(&self, data: &[u8]) -> Result<Vec<u8>, String> {
                Ok(data.to_ascii_uppercase())
            }
        }
        let sealed = IamOwnUsers::new(own.storage.clone()).with_sealer(Arc::new(Upper));
        let (first, _) = sealed.page(None, 1).await.unwrap();
        assert_eq!(first[0].1, b"__S4_IAM_USER_1".to_vec());
    }

    #[test]
    fn the_gate_stays_open() {
        let gate = IamAdoptionGate::default();
        assert!(!gate.is_open());
        let same = gate.clone();
        gate.open();
        assert!(same.is_open());
        assert!(IamAdoptionGate::opened().is_open());
    }
}
