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

//! Adoption of the bucket configurations an earlier release stored on single
//! nodes (plan BUCKET-CONFIG-REPLICATION.md, decision 5).
//!
//! Before protocol 6 a configuration was written to the storage of the node
//! that took the request, without an HLC, and that node is often not a
//! replica of the configuration's key: anti-entropy, which compares only the
//! keys placement gives both nodes of a pair, never sees it. A pass of
//! adoption takes every such record of this node — a configuration whose HLC
//! is made up ([`HlcTimestamp::is_made_up`]) — and brings it to the replicas
//! of its key:
//!
//! - A replica holding a version of a higher rank, a delete included, keeps
//!   it: of the values one configuration got on different nodes, the one
//!   changed last wins.
//! - A configuration whose bucket has no live marker of its generation
//!   ([`config_of_generation`]) belongs to a bucket deleted or created again
//!   since; it reads as not set (`read_bucket_config`) and is not spread.
//!   Every replica holding it, this node included, drops it: a copy left on a
//!   replica would be counted and warned about as a live configuration.
//! - Otherwise every replica behind it gets the copy, with its made-up HLC,
//!   whatever the number of replicas holding it: the node is the only witness
//!   of a write that was complete by the rules it was made under, and a copy
//!   on one node would otherwise be ambiguous forever. This holds only for
//!   records written before protocol 6 and only for configuration keys, before
//!   the pool's first switch to quorum reads. A later adoption also requires
//!   enough matching current owners or every absent owner's history horizon
//!   older than the copy: an earlier adopted copy may have missed a delete
//!   whose tombstone was collected meanwhile.
//!
//! A node that is not a replica of the key then removes its own copy, exactly
//! the generation it weighed; a copy that changed since it was listed is left
//! for a later pass. A key whose replicas do not all answer is left
//! for a later pass; a pass that left none is complete, and the node then
//! announces it in gossip (`GossipHandle::announce_bucket_configs_adopted`).
//! Configurations are read through the quorums once every node of the pool
//! has ([`ConfigReadGate`]). Passing again changes nothing that was settled.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use s4_core::storage::{VersionKind, VersionRank};
use s4_core::types::{BUCKET_CONFIG_KEY_PREFIXES, BUCKET_MARKER_KEY_PREFIX, SYSTEM_BUCKET};
use s4_core::{IndexRecord, StorageEngine};
use tracing::{debug, warn};

use crate::coordinator::bucket_config::{
    config_of_generation, within_clock_tolerance, LEGACY_CONFIG_CLOCK_TOLERANCE,
};
use crate::hlc::HlcTimestamp;
use crate::identity::NodeId;
use crate::repair::reconcile::{
    KeyRef, NewestVersion, Outcome, Reconciler, ReplicaHolds, StoredCopy,
};

/// Records listed from the storage at a time.
const PAGE: usize = 1000;

/// Whether this node reads bucket configurations through the quorums. Closed
/// until every node of the pool finished adoption: before that a
/// configuration may still sit only on a node that is not a replica of its
/// key, and a quorum read would miss it. Once open it stays open.
#[derive(Debug, Clone, Default)]
pub struct ConfigReadGate(Arc<AtomicBool>);

impl ConfigReadGate {
    /// A gate already open: for a node whose pool has nothing to adopt.
    pub fn opened() -> Self {
        let gate = Self::default();
        gate.open();
        gate
    }

    /// Whether configurations are read through the quorums.
    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Opens the gate, and says so in `s4_bucket_config_quorum_reads`.
    pub fn open(&self) {
        self.0.store(true, Ordering::Release);
        crate::cluster_metrics::set_config_quorum_reads(true);
    }
}

/// What one pass of adoption did, configuration by configuration.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AdoptionReport {
    /// Configurations now on every replica of their key, brought there or
    /// found there already.
    pub adopted: usize,
    /// Configurations outranked on the replicas by a newer version or a
    /// delete.
    pub superseded: usize,
    /// Configurations of a bucket deleted or created again since, which read
    /// as not set and were dropped from every replica holding them.
    pub orphaned: usize,
    /// Copies this node removed from its own storage: as it is not a replica
    /// of their key, or as they were orphaned.
    pub removed: usize,
    /// Configurations left for a later pass, with why.
    pub later: Vec<(String, String)>,
}

impl AdoptionReport {
    /// Whether every configuration of this node was settled.
    pub fn is_complete(&self) -> bool {
        self.later.is_empty()
    }
}

/// What became of one configuration.
#[derive(Debug, PartialEq, Eq)]
enum Fate {
    Adopted,
    Superseded,
    Orphaned,
}

/// Brings the configurations this node stored before protocol 6 to the
/// replicas of their keys.
pub struct ConfigAdoption {
    storage: Arc<dyn StorageEngine>,
    reconciler: Reconciler,
    local_node_id: NodeId,
    quorum_reads: bool,
}

impl ConfigAdoption {
    /// Adopts the configurations in `storage`, this node's own, reading and
    /// writing the replicas through `reconciler`.
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        reconciler: Reconciler,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            storage,
            reconciler,
            local_node_id,
            quorum_reads: false,
        }
    }

    /// Require evidence that a copy missed no delete after earlier adoption
    /// or an uncertain configuration-digest-aware restart. A surviving local
    /// copy may already have been adopted and deleted.
    pub fn after_quorum_reads(mut self) -> Self {
        self.quorum_reads = true;
        self
    }

    /// One pass over every configuration of this node with a made-up HLC.
    pub async fn pass(&self) -> AdoptionReport {
        let mut report = AdoptionReport::default();
        for prefix in BUCKET_CONFIG_KEY_PREFIXES {
            let mut after = String::new();
            loop {
                let page = match self
                    .storage
                    .list_objects_after(SYSTEM_BUCKET, prefix, &after, PAGE)
                    .await
                {
                    Ok(page) => page,
                    Err(e) => {
                        report.later.push((prefix.to_string(), format!("listing failed: {e}")));
                        break;
                    }
                };
                let Some((last, _)) = page.last() else {
                    break;
                };
                after = last.clone();
                let full = page.len() == PAGE;
                for (key, record) in page {
                    if !HlcTimestamp::of_record(&record).is_made_up() {
                        continue;
                    }
                    let Some(bucket) = key.strip_prefix(prefix) else {
                        continue;
                    };
                    match self.adopt(&key, bucket, &record, &mut report).await {
                        Ok(Fate::Adopted) => report.adopted += 1,
                        Ok(Fate::Superseded) => report.superseded += 1,
                        Ok(Fate::Orphaned) => report.orphaned += 1,
                        Err(why) => report.later.push((key, why)),
                    }
                }
                if !full {
                    break;
                }
            }
        }
        crate::cluster_metrics::record_config_adoption(
            report.adopted as u64,
            report.superseded as u64,
            report.orphaned as u64,
            report.later.len() as u64,
        );
        report
    }

    /// Settles one configuration, `record` as this node holds it.
    async fn adopt(
        &self,
        key: &str,
        bucket: &str,
        record: &IndexRecord,
        report: &mut AdoptionReport,
    ) -> Result<Fate, String> {
        let own_hlc = HlcTimestamp::of_record(record);
        let own = VersionRank::new(own_hlc.as_tuple(), VersionKind::Live, &record.etag);
        let source = self.reconciler.source();
        let versions = source
            .key_versions(SYSTEM_BUCKET, key)
            .await
            .map_err(|e| format!("its replicas could not be asked: {e}"))?;
        if let Some((node, why)) = versions.unanswered.first() {
            return Err(format!("replica {node} did not answer: {why}"));
        }
        let replica = versions.replicas.contains(&self.local_node_id);
        let others = || versions.answers.iter().filter(|(node, _)| *node != self.local_node_id);

        let outcome = if others().filter_map(|(_, holds)| holds.rank()).any(|rank| rank > own) {
            Fate::Superseded
        } else if !self.belongs_to_live_bucket(bucket, own_hlc).await? {
            let config = KeyRef {
                bucket: SYSTEM_BUCKET,
                key,
            };
            for (target, holds) in others() {
                if holds.rank().is_none() {
                    continue;
                }
                match self.reconciler.remove_stale(*target, config, own_hlc).await {
                    Ok(Outcome::Delivered) => {}
                    Ok(other) => return Err(format!("replica {target}: {other:?}")),
                    Err(e) => return Err(format!("replica {target}: {e}")),
                }
            }
            Fate::Orphaned
        } else {
            if self.quorum_reads {
                let held = versions
                    .answers
                    .iter()
                    .filter(|(node, holds)| {
                        versions.confirming_replicas.contains(node) && holds.rank() == Some(own)
                    })
                    .count();
                let no_missed_delete = versions.answers.iter().all(|(_, holds)| match holds {
                    ReplicaHolds::Nothing { horizon } => {
                        horizon.is_some_and(|horizon| horizon < own_hlc)
                    }
                    ReplicaHolds::Version { .. } => true,
                });
                if held < versions.confirming.max(1) && !no_missed_delete {
                    return Err("previously adopted copy may have missed a collected delete; waiting for history confirmation".into());
                }
            }
            let copy = self.own_copy(key, own_hlc).await?;
            let config = KeyRef {
                bucket: SYSTEM_BUCKET,
                key,
            };
            for (target, holds) in others() {
                if holds.rank().is_some_and(|held| held >= own) {
                    continue;
                }
                match self.reconciler.deliver_copy(*target, config, copy.clone()).await {
                    Ok(Outcome::Delivered) => {}
                    Ok(other) => return Err(format!("replica {target}: {other:?}")),
                    Err(e) => return Err(format!("replica {target}: {e}")),
                }
            }
            Fate::Adopted
        };

        if !replica || outcome == Fate::Orphaned {
            let removed = self
                .storage
                .delete_object_if_hlc(
                    SYSTEM_BUCKET,
                    key,
                    record.hlc_timestamp,
                    record.hlc_logical,
                    record.origin_node_id,
                )
                .await
                .map_err(|e| format!("the local copy could not be removed: {e}"))?;
            if !removed {
                return Err("the local copy changed while it was adopted".into());
            }
            report.removed += 1;
        }
        debug!(key, ?outcome, replica, "bucket configuration adopted");
        Ok(outcome)
    }

    /// Whether the bucket has a live marker of the configuration's
    /// generation ([`config_of_generation`]): one the configuration was
    /// written to, not one created since.
    async fn belongs_to_live_bucket(
        &self,
        bucket: &str,
        hlc: HlcTimestamp,
    ) -> Result<bool, String> {
        let marker_key = format!("{BUCKET_MARKER_KEY_PREFIX}{bucket}");
        let marker = self
            .reconciler
            .source()
            .key_versions(SYSTEM_BUCKET, &marker_key)
            .await
            .map_err(|e| format!("the bucket's marker could not be read: {e}"))?;
        if let Some((node, why)) = marker.unanswered.first() {
            return Err(format!(
                "replica {node} of the bucket's marker did not answer: {why}"
            ));
        }
        let NewestVersion::Copy {
            hlc: marker_hlc, ..
        } = marker.newest()
        else {
            return Ok(false);
        };
        if within_clock_tolerance(hlc, marker_hlc) {
            warn!(
                bucket,
                config_hlc = %hlc,
                marker_hlc = %marker_hlc,
                tolerance_ms = LEGACY_CONFIG_CLOCK_TOLERANCE.as_millis() as u64,
                "A bucket configuration stored by an earlier release is no newer than its \
                 bucket's marker, by less than the tolerated clock skew: taken as the bucket's. \
                 If the bucket was deleted and created again within that time, check the \
                 configuration"
            );
        }
        Ok(config_of_generation(hlc, marker_hlc))
    }

    /// This node's copy of `key`, as a repair sends it: the generation of
    /// `hlc` it was weighed as, or `Err` when the copy changed since.
    async fn own_copy(&self, key: &str, hlc: HlcTimestamp) -> Result<StoredCopy, String> {
        let (data, record) = self
            .storage
            .get_object(SYSTEM_BUCKET, key)
            .await
            .map_err(|e| format!("the local copy could not be read: {e}"))?;
        if HlcTimestamp::of_record(&record) != hlc {
            return Err("the local copy changed while it was adopted".into());
        }
        Ok(StoredCopy {
            hlc: Some(hlc),
            data,
            etag: record.etag,
            content_hash: record.content_hash.to_vec(),
            content_type: record.content_type,
            metadata: record.metadata,
            version_id: record.version_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use s4_core::KeyVersion;

    use super::*;
    use crate::coordinator::read::QuorumReadCoordinator;
    use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};
    use crate::repair::reconcile::{KeyStateSource, KeyVersions};

    const CORS: &str = "__s4_bucket_cors_bucket";

    /// The pool's placement with node 1 taken out of every key's replicas:
    /// node 1 as a node of a larger pool that is no replica of the
    /// configurations it holds.
    struct NotAReplica(QuorumReadCoordinator);

    #[async_trait::async_trait]
    impl KeyStateSource for NotAReplica {
        async fn newest_version(
            &self,
            bucket: &str,
            key: &str,
            target: NodeId,
        ) -> Result<NewestVersion, crate::error::ClusterError> {
            self.0.newest_version(bucket, key, target).await
        }

        async fn key_versions(
            &self,
            bucket: &str,
            key: &str,
        ) -> Result<KeyVersions, crate::error::ClusterError> {
            let mut versions = self.0.key_versions(bucket, key).await?;
            versions.replicas.retain(|node| *node != NodeId(1));
            versions.answers.retain(|(node, _)| *node != NodeId(1));
            versions.unanswered.retain(|(node, _)| *node != NodeId(1));
            versions.confirming_replicas.retain(|node| *node != NodeId(1));
            Ok(versions)
        }

        async fn read_copy(
            &self,
            bucket: &str,
            key: &str,
            node: NodeId,
        ) -> Result<StoredCopy, crate::error::ClusterError> {
            self.0.read_copy(bucket, key, node).await
        }
    }

    /// Adoption on node 1, a replica of every key.
    fn as_replica(pool: &TestPool) -> ConfigAdoption {
        ConfigAdoption::new(pool.storage(1).clone(), pool.reconciler(), NodeId(1))
    }

    /// Adoption on node 1 as a node that is no replica of any key.
    fn as_non_replica(pool: &TestPool) -> ConfigAdoption {
        let reconciler = Reconciler::new(
            Arc::new(NotAReplica(pool.read_coordinator())),
            pool.rpc_client.clone(),
            pool.gossip.clone(),
            NodeId(1),
            pool.epoch(),
        );
        ConfigAdoption::new(pool.storage(1).clone(), reconciler, NodeId(1))
    }

    /// Node `node` stores `data` as the CORS configuration of [`BUCKET`] the
    /// way a release before protocol 6 did: locally, without an HLC.
    async fn stored_before_protocol_6(pool: &TestPool, node: u128, data: &[u8]) {
        pool.storage(node)
            .put_object("__system__", CORS, data, "application/xml", &HashMap::new())
            .await
            .unwrap();
        // Records stored one after another differ in their modification time.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    /// What node `node` holds of the configuration: its bytes, or `None`.
    async fn held(pool: &TestPool, node: u128) -> Option<Vec<u8>> {
        pool.storage(node)
            .get_object("__system__", CORS)
            .await
            .ok()
            .map(|(data, _)| data)
    }

    /// The bucket's marker on every node, written long before any
    /// configuration.
    async fn bucket_on_every_node(pool: &TestPool) {
        for node in 1..=3 {
            pool.create_bucket(node, at(10)).await;
        }
    }

    /// A previously adopted legacy copy can retain its made-up HLC on a
    /// former owner. After the owners collect its delete, readoption must
    /// retain the ambiguity rather than resurrect the configuration.
    #[tokio::test]
    async fn readoption_does_not_spread_a_foreign_legacy_copy_after_delete_gc() {
        use s4_core::storage::TombstoneRef;

        let pool = TestPool::start(Replica::Up, Replica::Up, 21358).await;
        bucket_on_every_node(&pool).await;
        stored_before_protocol_6(&pool, 1, b"old legacy configuration").await;
        let (_, record) = pool.storage(1).get_object(SYSTEM_BUCKET, CORS).await.unwrap();
        let hlc = HlcTimestamp::of_record(&record);
        let deleted = at(hlc.wall_time + 1);
        for node in 2..=3 {
            pool.put_system(node, CORS, b"old legacy configuration", hlc).await;
            pool.delete_system(node, CORS, deleted).await;
            assert_eq!(
                pool.storage(node)
                    .gc_tombstones(&[TombstoneRef {
                        full_key: format!("{SYSTEM_BUCKET}/{CORS}"),
                        hlc: deleted.as_tuple(),
                    }])
                    .await
                    .unwrap(),
                1
            );
        }
        let report = as_non_replica(&pool).after_quorum_reads().pass().await;
        assert_eq!(report.later.len(), 1, "{report:?}");
        assert_eq!(report.adopted, 0);
        assert_eq!(
            report.removed, 0,
            "an ambiguous copy is retained for investigation"
        );
        for node in 2..=3 {
            assert_eq!(
                held(&pool, node).await,
                None,
                "node {node} resurrected the configuration"
            );
        }

        // Independent confirmation on the current owners makes a later pass
        // safe even though their purge horizons remain above this copy.
        for node in 2..=3 {
            pool.put_system(node, CORS, b"old legacy configuration", hlc).await;
        }
        let report = as_non_replica(&pool).after_quorum_reads().pass().await;
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(report.adopted, 1);
        assert_eq!(report.removed, 1);
    }

    /// A legacy write newer than every missing owner's history boundary can
    /// still be adopted after a downgrade: no delete of that copy was lost.
    #[tokio::test]
    async fn readoption_spreads_a_copy_newer_than_every_absent_owners_horizon() {
        use s4_core::storage::TombstoneRef;

        let pool = TestPool::start(Replica::Up, Replica::Up, 21362).await;
        bucket_on_every_node(&pool).await;
        for node in 2..=3 {
            pool.delete_system(node, CORS, at(20)).await;
            assert_eq!(
                pool.storage(node)
                    .gc_tombstones(&[TombstoneRef {
                        full_key: format!("{SYSTEM_BUCKET}/{CORS}"),
                        hlc: at(20).as_tuple(),
                    }])
                    .await
                    .unwrap(),
                1
            );
        }
        stored_before_protocol_6(&pool, 1, b"a later legacy write").await;
        let report = as_non_replica(&pool).after_quorum_reads().pass().await;
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(report.adopted, 1);
        assert_eq!(report.removed, 1);
        for node in 2..=3 {
            assert_eq!(
                held(&pool, node).await.as_deref(),
                Some(b"a later legacy write".as_slice())
            );
        }
    }

    /// A configuration on a node that is no replica of its key reaches every
    /// replica and leaves that node; a second pass finds nothing left to do.
    #[tokio::test]
    async fn a_configuration_on_a_node_that_is_no_replica_is_spread_and_removed_there() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let pool = TestPool::start(Replica::Up, Replica::Up, 21300).await;
        bucket_on_every_node(&pool).await;
        stored_before_protocol_6(&pool, 1, b"<CORSConfiguration>one</CORSConfiguration>").await;

        let adoption = as_non_replica(&pool);
        let report = adoption.pass().await;
        assert_eq!(
            report,
            AdoptionReport {
                adopted: 1,
                removed: 1,
                ..AdoptionReport::default()
            }
        );
        for node in 2..=3 {
            assert_eq!(
                held(&pool, node).await.as_deref(),
                Some(&b"<CORSConfiguration>one</CORSConfiguration>"[..]),
                "node {node}"
            );
        }
        assert_eq!(held(&pool, 1).await, None);
        assert!(
            matches!(
                pool.storage(1).key_version("__system__", CORS).await.unwrap(),
                KeyVersion::Absent
            ),
            "node 1 removed its copy without a tombstone"
        );

        assert_eq!(adoption.pass().await, AdoptionReport::default());
        let adoption_keys = |outcome| {
            metrics.value(
                "s4_bucket_config_adoption_keys_total",
                &[("outcome", outcome)],
            )
        };
        assert_eq!(adoption_keys("adopted"), Some(1.0));
        assert_eq!(adoption_keys("deferred"), Some(0.0));
    }

    /// Of two values one configuration got on different nodes, the one
    /// changed last wins, whichever node adopts first.
    #[tokio::test]
    async fn of_two_values_the_one_changed_last_wins() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21304).await;
        bucket_on_every_node(&pool).await;
        stored_before_protocol_6(&pool, 1, b"older").await;
        stored_before_protocol_6(&pool, 2, b"newer").await;

        let report = as_non_replica(&pool).pass().await;
        assert_eq!(
            report,
            AdoptionReport {
                superseded: 1,
                removed: 1,
                ..AdoptionReport::default()
            }
        );
        assert_eq!(held(&pool, 1).await, None);
        assert_eq!(held(&pool, 2).await.as_deref(), Some(&b"newer"[..]));
        assert_eq!(held(&pool, 3).await, None, "the older value was not spread");

        // The other order: the newer value is the adopting node's.
        let pool = TestPool::start(Replica::Up, Replica::Up, 21308).await;
        bucket_on_every_node(&pool).await;
        stored_before_protocol_6(&pool, 2, b"older").await;
        stored_before_protocol_6(&pool, 1, b"newer").await;
        let report = as_non_replica(&pool).pass().await;
        assert_eq!(
            report,
            AdoptionReport {
                adopted: 1,
                removed: 1,
                ..AdoptionReport::default()
            }
        );
        for node in 2..=3 {
            assert_eq!(
                held(&pool, node).await.as_deref(),
                Some(&b"newer"[..]),
                "node {node}"
            );
        }
    }

    /// A configuration of a bucket deleted since, or deleted and created
    /// again, is not spread: it reads as not set anyway. The node that is no
    /// replica of it drops it.
    #[tokio::test]
    async fn a_configuration_of_a_bucket_gone_since_is_not_spread() {
        let marker = format!("{BUCKET_MARKER_KEY_PREFIX}{BUCKET}");
        let pool = TestPool::start(Replica::Up, Replica::Up, 21312).await;
        bucket_on_every_node(&pool).await;
        stored_before_protocol_6(&pool, 1, b"of a deleted bucket").await;
        let later = HlcTimestamp::from_record(crate::clock::hlc::now_millis() + 10_000, 0, 7);
        for node in 1..=3 {
            pool.delete_system(node, &marker, later).await;
        }
        let report = as_non_replica(&pool).pass().await;
        assert_eq!(
            report,
            AdoptionReport {
                orphaned: 1,
                removed: 1,
                ..AdoptionReport::default()
            }
        );
        for node in 1..=3 {
            assert_eq!(held(&pool, node).await, None, "node {node}");
        }

        let pool = TestPool::start(Replica::Up, Replica::Up, 21316).await;
        stored_before_protocol_6(&pool, 1, b"of the bucket before").await;
        let created_again =
            HlcTimestamp::from_record(crate::clock::hlc::now_millis() + 10_000, 0, 7);
        for node in 1..=3 {
            pool.create_bucket(node, created_again).await;
        }
        let report = as_non_replica(&pool).pass().await;
        assert_eq!((report.orphaned, report.removed), (1, 1), "{report:?}");
        for node in 2..=3 {
            assert_eq!(held(&pool, node).await, None, "node {node}");
        }
    }

    /// A configuration stored a moment after its bucket was created, by a
    /// node whose clock runs behind the one that created it, belongs to the
    /// bucket: it is spread, not dropped as one of an earlier bucket.
    #[tokio::test]
    async fn a_configuration_stored_by_a_clock_running_behind_is_adopted() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21336).await;
        let created_ahead = HlcTimestamp::from_record(crate::clock::hlc::now_millis() + 50, 0, 7);
        for node in 1..=3 {
            pool.create_bucket(node, created_ahead).await;
        }
        stored_before_protocol_6(&pool, 1, b"right after CreateBucket").await;
        let report = as_non_replica(&pool).pass().await;
        assert_eq!(
            report,
            AdoptionReport {
                adopted: 1,
                removed: 1,
                ..AdoptionReport::default()
            }
        );
        for node in 2..=3 {
            assert_eq!(
                held(&pool, node).await.as_deref(),
                Some(&b"right after CreateBucket"[..]),
                "node {node}"
            );
        }
    }

    /// A replica holding a configuration of an earlier bucket drops it, and
    /// so does every other replica holding that generation or an older one:
    /// nothing is left to be counted as a live configuration or spread again.
    #[tokio::test]
    async fn an_orphaned_configuration_leaves_every_replica() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21340).await;
        stored_before_protocol_6(&pool, 2, b"older, of the bucket before").await;
        stored_before_protocol_6(&pool, 1, b"of the bucket before").await;
        let created_again =
            HlcTimestamp::from_record(crate::clock::hlc::now_millis() + 10_000, 0, 7);
        for node in 1..=3 {
            pool.create_bucket(node, created_again).await;
        }
        let report = as_replica(&pool).pass().await;
        assert_eq!(
            report,
            AdoptionReport {
                orphaned: 1,
                removed: 1,
                ..AdoptionReport::default()
            }
        );
        for node in 1..=3 {
            assert_eq!(held(&pool, node).await, None, "node {node}");
        }
    }

    /// A copy that changed between the listing and its adoption is not sent
    /// under the generation listed: the key waits for a later pass.
    #[tokio::test]
    async fn a_copy_changed_while_it_is_adopted_waits_for_a_later_pass() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21344).await;
        bucket_on_every_node(&pool).await;
        stored_before_protocol_6(&pool, 1, b"listed").await;
        let (_, listed) = pool.storage(1).get_object("__system__", CORS).await.unwrap();
        let quorum = HlcTimestamp::from_record(crate::clock::hlc::now_millis() + 1000, 0, 7);
        pool.put_system(1, CORS, b"written through the quorums", quorum).await;

        let adoption = as_replica(&pool);
        let mut report = AdoptionReport::default();
        let fate = adoption.adopt(CORS, BUCKET, &listed, &mut report).await;
        assert_eq!(
            fate,
            Err("the local copy changed while it was adopted".to_string())
        );
        for node in 2..=3 {
            assert_eq!(held(&pool, node).await, None, "node {node}");
        }
    }

    /// A replica of the configuration brings the others to its copy and
    /// keeps its own; a pass again writes nothing, and a replica that holds
    /// a delete newer than the copy keeps the delete.
    #[tokio::test]
    async fn a_replica_spreads_its_copy_and_a_second_pass_changes_nothing() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21320).await;
        bucket_on_every_node(&pool).await;
        stored_before_protocol_6(&pool, 1, b"kept").await;
        let adoption = as_replica(&pool);
        assert_eq!(
            adoption.pass().await,
            AdoptionReport {
                adopted: 1,
                ..AdoptionReport::default()
            }
        );
        let mut versions = Vec::new();
        for node in 1..=3 {
            assert_eq!(
                held(&pool, node).await.as_deref(),
                Some(&b"kept"[..]),
                "node {node}"
            );
            versions.push(format!(
                "{:?}",
                pool.storage(node).key_version("__system__", CORS).await.unwrap()
            ));
        }
        assert_eq!(
            adoption.pass().await,
            AdoptionReport {
                adopted: 1,
                ..AdoptionReport::default()
            }
        );
        for node in 1..=3 {
            assert_eq!(
                format!(
                    "{:?}",
                    pool.storage(node).key_version("__system__", CORS).await.unwrap()
                ),
                versions[node as usize - 1],
                "node {node}"
            );
        }

        let pool = TestPool::start(Replica::Up, Replica::Up, 21324).await;
        bucket_on_every_node(&pool).await;
        stored_before_protocol_6(&pool, 1, b"deleted later").await;
        let later = HlcTimestamp::from_record(crate::clock::hlc::now_millis() + 1000, 0, 7);
        pool.delete_system(2, CORS, later).await;
        let report = as_replica(&pool).pass().await;
        assert_eq!(report.superseded, 1, "{report:?}");
        assert!(matches!(
            pool.storage(2).key_version("__system__", CORS).await.unwrap(),
            KeyVersion::Tombstone(_)
        ));
        assert_eq!(held(&pool, 3).await, None);
    }

    /// A replica that does not answer leaves the configuration for a later
    /// pass: nothing is written and the node keeps its copy.
    #[tokio::test]
    async fn a_replica_that_does_not_answer_leaves_the_configuration_for_later() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let pool = TestPool::start(Replica::Up, Replica::Down, 21328).await;
        bucket_on_every_node(&pool).await;
        stored_before_protocol_6(&pool, 1, b"waits").await;
        let report = as_non_replica(&pool).pass().await;
        assert!(!report.is_complete(), "{report:?}");
        assert_eq!(report.later.len(), 1, "{report:?}");
        assert_eq!(report.later[0].0, CORS);
        assert_eq!(held(&pool, 1).await.as_deref(), Some(&b"waits"[..]));
        assert_eq!(held(&pool, 2).await, None);
        as_non_replica(&pool).pass().await;
        assert_eq!(
            metrics.value(
                "s4_bucket_config_adoption_keys_total",
                &[("outcome", "deferred")]
            ),
            Some(2.0),
            "a key left for later is counted by each pass"
        );
        assert_eq!(
            metrics.value(
                "s4_bucket_config_adoption_keys_total",
                &[("outcome", "adopted")]
            ),
            Some(0.0)
        );
    }

    /// The gate says in `s4_bucket_config_quorum_reads` when it opens.
    #[test]
    fn an_open_gate_says_so_in_the_metrics() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let gate = ConfigReadGate::default();
        assert!(!gate.is_open());
        assert_eq!(metrics.value("s4_bucket_config_quorum_reads", &[]), None);
        gate.open();
        assert!(gate.is_open());
        assert_eq!(
            metrics.value("s4_bucket_config_quorum_reads", &[]),
            Some(1.0)
        );
    }

    /// Configurations written through the quorums carry an HLC a node issued
    /// and are no adoption's business.
    #[tokio::test]
    async fn a_configuration_written_through_the_quorums_is_left_alone() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21332).await;
        bucket_on_every_node(&pool).await;
        pool.put_system(1, CORS, b"quorum", at(20)).await;
        assert_eq!(
            as_non_replica(&pool).pass().await,
            AdoptionReport::default()
        );
        assert_eq!(held(&pool, 1).await.as_deref(), Some(&b"quorum"[..]));
        assert_eq!(held(&pool, 2).await, None);
    }
}
