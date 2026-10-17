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

//! Exhaustive acknowledgement/read intersection model for EPGR-02-6.
//!
//! A value and its tombstone have the same holders, so the intersection proof
//! applies to both. This model proves quorum visibility, not backfill completeness
//! or LWW ordering; those require separate storage and migration tests.

use super::{
    BucketMetadataMove, BucketPlacement, ClassLayout, PlacementRouter, PlacementStrategy, WriteRule,
};
use crate::config::{ClusterConfig, PoolConfig};
use crate::identity::{NodeAddr, NodeId, PoolId};
use crate::metadata_placement::{Stage, StageCell};
use crate::pool_generation::PoolGenerationRecord;
use std::collections::{HashMap, HashSet};

fn moving_router(old_count: usize, metadata_rf: u8) -> PlacementRouter {
    let old = (1..=old_count as u128).map(NodeId).collect::<Vec<_>>();
    let addresses = (0..old_count)
        .map(|i| {
            let addr = |base| format!("127.0.0.1:{}", base + i).parse().unwrap();
            NodeAddr {
                grpc_addr: addr(9100),
                http_addr: addr(10100),
                gossip_addr: addr(11100),
            }
        })
        .collect();
    let pool = PoolConfig {
        pool_id: PoolId(1),
        name: "model".into(),
        nodes: addresses,
        replication_factor: Some(3),
        write_quorum: Some(2),
        read_quorum: Some(2),
        storage_class: s4_core::types::StorageClass::Standard,
    };
    let config = ClusterConfig {
        pools: vec![pool],
        ..ClusterConfig::default()
    };
    let router = PlacementRouter::from_pool_node_ids(
        &config,
        NodeId(1),
        BucketPlacement::new(),
        &HashMap::from([(PoolId(1), old.clone())]),
    )
    .unwrap();
    let data = ClassLayout::new(PlacementStrategy::new(old.clone(), 3).unwrap(), 2, 2).unwrap();
    let metadata = ClassLayout::new(
        PlacementStrategy::new(old.clone(), metadata_rf).unwrap(),
        metadata_rf / 2 + 1,
        metadata_rf / 2 + 1,
    )
    .unwrap();
    let router = router
        .with_bucket_metadata_move(
            PoolId(1),
            BucketMetadataMove::new(data, metadata, StageCell::new(Stage::Settled)),
        )
        .unwrap();
    let record = PoolGenerationRecord::initial(PoolId(1), old)
        .unwrap()
        .configure((1..=old_count as u128 * 2).map(NodeId).collect())
        .unwrap()
        .begin()
        .unwrap();
    router.with_pool_generation(&record).unwrap()
}

fn mask(ids: &[NodeId]) -> u16 {
    ids.iter().fold(0, |mask, node| mask | (1 << (node.0 - 1)))
}

/// No sampling: enumerate every subset of the physical pool, including extra
/// acknowledgements and read responses beyond the minimum quorum.
fn prove_rule(
    rule: &WriteRule,
    old_read: (u16, u8),
    new_read: (u16, u8),
    count: usize,
) -> (u64, u64) {
    let reads = [old_read, new_read].map(|(owners, quorum)| {
        (0u16..(1 << count))
            .filter(|reply| reply & !owners == 0 && reply.count_ones() >= u32::from(quorum))
            .collect::<Vec<_>>()
    });
    let mut intersections = 0;
    let mut negative_misses = 0;
    let owners = old_read.0 | new_read.0;
    for ack in 0u16..(1 << count) {
        if ack & !owners != 0 {
            continue;
        }
        let acknowledged = nodes(ack, count);
        // Negative control: remove the successor requirement from the actual
        // production rule, then run the same complete acknowledgement space.
        if WriteRule::quorum(&rule.requirements()[0].nodes, rule.requirements()[0].count)
            .met(&acknowledged)
        {
            negative_misses += reads[1].iter().filter(|reply| ack & **reply == 0).count() as u64;
        }
        if !rule.met(&acknowledged) {
            continue;
        }
        for reply in reads.iter().flatten() {
            assert_ne!(
                ack & reply,
                0,
                "write {ack:#x} misses read {reply:#x}: {rule:?}"
            );
            intersections += 1;
        }
    }
    (intersections, negative_misses)
}

#[test]
fn joint_generation_quorums_exhaustively_intersect_old_and_new_reads() {
    for (old_count, metadata_rf) in [(3, 3), (6, 3), (6, 5)] {
        let router = moving_router(old_count, metadata_rf);
        let ranges = router.generation_ranges(&PoolId(1)).unwrap();
        let mut seen = HashSet::new();
        let mut intersections = 0;
        let mut negative_misses = 0;
        let snapshot = ranges.snapshot();
        assert_eq!(snapshot.len(), old_count * 2 * 128);
        for range in &snapshot {
            let mut rules = Vec::new();
            for (bucket, key) in [
                ("bucket", "object"),
                ("__system__", "__s4_bucket_marker_bucket"),
                ("__system__", "__s4_iam_v2_user_model"),
            ] {
                let before = router
                    .route_at_position(PoolId(1), bucket, key, range.upper_inclusive)
                    .unwrap();
                assert_eq!(before.write_rule.requirements().len(), 2);
                rules.push(before.write_rule.clone());
                let old = &before.write_rule.requirements()[0];
                let new = &before.write_rule.requirements()[1];
                assert_eq!(mask(&before.read_replicas), mask(&old.nodes));
                let signature = (
                    mask(&old.nodes),
                    old.count,
                    mask(&new.nodes),
                    new.count,
                    before.read_quorum,
                );
                if seen.insert(signature) {
                    let (checks, misses) = prove_rule(
                        &before.write_rule,
                        (signature.0, before.read_quorum),
                        (signature.2, new.count),
                        old_count * 2,
                    );
                    intersections += checks;
                    negative_misses += misses;
                }
            }
            for state in [
                super::RangeState::Streaming,
                super::RangeState::Verified,
                super::RangeState::CutOver,
            ] {
                ranges.transition(range.upper_inclusive, state).unwrap();
                for (index, (bucket, key)) in [
                    ("bucket", "object"),
                    ("__system__", "__s4_bucket_marker_bucket"),
                    ("__system__", "__s4_iam_v2_user_model"),
                ]
                .into_iter()
                .enumerate()
                {
                    let decision = router
                        .route_at_position(PoolId(1), bucket, key, range.upper_inclusive)
                        .unwrap();
                    assert_eq!(
                        decision.write_rule, rules[index],
                        "local cutover must retain both write quorums"
                    );
                    let side = usize::from(state == super::RangeState::CutOver);
                    let expected = &decision.write_rule.requirements()[side];
                    assert_eq!(mask(&decision.read_replicas), mask(&expected.nodes));
                    assert_eq!(decision.read_quorum, expected.count);
                }
            }
        }
        eprintln!("EPGR-02-6 {old_count}->{} RF_meta={metadata_rf}: {} arcs, {} distinct quorum pairs, {intersections} ack/read intersections, {negative_misses} negative-control misses",
            old_count * 2, snapshot.len(), seen.len());
        assert!(intersections > 0);
        assert!(
            negative_misses > 0,
            "disabled joint quorum must fail the same enumeration"
        );
    }
}

fn nodes(mask: u16, count: usize) -> Vec<NodeId> {
    (0..count)
        .filter(|bit| mask & (1 << bit) != 0)
        .map(|bit| NodeId(bit as u128 + 1))
        .collect()
}

#[test]
fn local_cutover_with_new_only_writes_has_a_concrete_counterexample() {
    let old = [NodeId(1), NodeId(2), NodeId(3)];
    let new = [NodeId(1), NodeId(4), NodeId(5)];
    let ack = [NodeId(4), NodeId(5)];
    let read = [NodeId(1), NodeId(2)];
    assert!(WriteRule::quorum(&new, 2).met(&ack));
    assert!(WriteRule::quorum(&old, 2).met(&read));
    assert!(ack.iter().all(|node| !read.contains(node)));
}

fn factorial(n: usize) -> u64 {
    (1..=n as u64).product()
}

/// Each subset S is the prefix of exactly |S|! (N-|S|)! permutations:
/// arrange S first, then its complement. Thus enumerating all subsets covers
/// every prefix of every cutover order, with no sampled or assumed order.
#[test]
fn every_coordinator_cutover_order_is_represented_by_its_prefix_set() {
    for count in [6, 12] {
        let mut prefixes = 0;
        let mut combinations = [[false; 2]; 2];
        for state in 0u16..(1 << count) {
            let cut = state.count_ones() as usize;
            prefixes += factorial(cut) * factorial(count - cut);
            for writer in 0..count {
                for reader in 0..count {
                    combinations[usize::from(state & (1 << writer) != 0)]
                        [usize::from(state & (1 << reader) != 0)] = true;
                }
            }
            assert_eq!(nodes(state, count).len(), cut);
        }
        assert_eq!(prefixes, (count as u64 + 1) * factorial(count));
        assert_eq!(combinations, [[true; 2]; 2]);
    }
}
