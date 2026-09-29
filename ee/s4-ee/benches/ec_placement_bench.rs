// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Erasure set routing baseline for EC v2 plan 03 phase 5 (task EC2-MSET-05-006).
//!
//! Measures the runtime cost of `EcPlacementService::select_write_set`, which
//! picks an erasure set before object encoding starts and enforces the
//! rolling-upgrade gate.

#![allow(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use s4_cluster::{MockClusterMembership, NodeId, PoolId};
use s4_ee::ec_placement::{
    EcCapacitySource, EcNodeCapacity, EcPlacementConfig, EcPlacementService,
};
use s4_ee::erasure_coding::{
    EcPlacementStrategy, EcProfile, EcTopologyPolicy, ErasureSet, ErasureSetRegistry,
};

/// Capacity source returning the same measured, fresh numbers for every node.
struct FixedCapacity {
    capacity: EcNodeCapacity,
}

impl FixedCapacity {
    fn fresh() -> Self {
        Self {
            capacity: EcNodeCapacity {
                total_bytes: 1_000_000_000_000,
                available_bytes: 500_000_000_000,
                reported_ago: Duration::from_secs(1),
            },
        }
    }
}

impl EcCapacitySource for FixedCapacity {
    fn node_capacity(&self, _node_id: NodeId) -> Option<EcNodeCapacity> {
        Some(self.capacity)
    }
}

/// Builds an erasure set registry of `n` sets for pool 0.
///
/// The first set is the inherited set (EC v1 identity), and any subsequent sets
/// are active sets with sequential IDs starting at 1. Sets of a pool never share
/// a node, so each set receives a distinct block of six nodes.
fn build_pool(n: usize) -> ErasureSetRegistry {
    assert!(n >= 1, "a pool must have at least one set");
    let pool_id = PoolId::new(0);
    let profile = EcProfile::rs_small();
    let width = usize::from(profile.total_shards());
    let empty_labels = HashMap::new();

    let mut sets = Vec::with_capacity(n);
    for i in 0..n {
        let start_node = (i * width + 1) as u128;
        let nodes: Vec<NodeId> = (start_node..start_node + width as u128).map(NodeId).collect();

        let set = if i == 0 {
            ErasureSet::inherited(
                pool_id,
                profile.clone(),
                &nodes,
                &empty_labels,
                EcTopologyPolicy::Strict,
                1,
                1,
            )
            .expect("inherited set must build")
        } else {
            ErasureSet::active(
                pool_id,
                i as u64,
                profile.clone(),
                &nodes,
                &empty_labels,
                EcTopologyPolicy::Strict,
                1,
                1,
            )
            .expect("active set must build")
        };
        sets.push(set);
    }

    ErasureSetRegistry::from_sets(pool_id, sets).expect("registry must build")
}

/// Builds an `EcPlacementService` with multi-set placement enabled and optional
/// capacity source and cluster membership wiring.
fn build_service(
    sets: ErasureSetRegistry,
    strategy: EcPlacementStrategy,
    wire_capacity: bool,
    wire_membership: bool,
) -> EcPlacementService {
    let defaults = EcPlacementConfig::default();
    let config = EcPlacementConfig {
        multi_set_enabled: true,
        strategy,
        full_threshold: defaults.full_threshold,
        capacity_max_age: defaults.capacity_max_age,
    };
    let service = EcPlacementService::new(sets, config);
    if wire_capacity {
        service.configure_capacity_source(Arc::new(FixedCapacity::fresh()));
    }
    if wire_membership {
        service.configure_membership(Arc::new(MockClusterMembership::new(true)));
    }
    service
}

/// Measures round-robin set selection across pools of 1, 2, 4, and 8 sets with no capacity source wired.
///
/// This represents the default configuration of a multi-set pool in production today.
fn bench_round_robin(c: &mut Criterion) {
    let mut group = c.benchmark_group("select/round_robin");
    for set_count in [1, 2, 4, 8] {
        let service = build_service(
            build_pool(set_count),
            EcPlacementStrategy::RoundRobin,
            false,
            false,
        );
        group.bench_function(BenchmarkId::from_parameter(set_count), |b| {
            b.iter(|| black_box(service.select_write_set().expect("placement must succeed")));
        });
    }
    group.finish();
}

/// Measures free-space weighted set selection across pools of 2 and 8 sets with a fresh capacity source.
///
/// The weighted walk tracks per-set credits and recalculates weights, representing the upper bound
/// of routing overhead between the two strategies.
fn bench_free_space(c: &mut Criterion) {
    let mut group = c.benchmark_group("select/free_space");
    for set_count in [2, 8] {
        let service = build_service(
            build_pool(set_count),
            EcPlacementStrategy::FreeSpace,
            true,
            false,
        );
        group.bench_function(BenchmarkId::from_parameter(set_count), |b| {
            b.iter(|| black_box(service.select_write_set().expect("placement must succeed")));
        });
    }
    group.finish();
}

/// Measures the rolling-upgrade gate overhead on a 2-set pool using round-robin.
///
/// Comparing a service wired with `MockClusterMembership::new(true)` against an unwired service
/// isolates the per-object cost of verifying that pool nodes support EC v2 before writing.
fn bench_upgrade_gate(c: &mut Criterion) {
    let mut group = c.benchmark_group("select/upgrade_gate");

    let service_wired = build_service(build_pool(2), EcPlacementStrategy::RoundRobin, false, true);
    group.bench_function("wired", |b| {
        b.iter(|| black_box(service_wired.select_write_set().expect("placement must succeed")));
    });

    let service_unwired =
        build_service(build_pool(2), EcPlacementStrategy::RoundRobin, false, false);
    group.bench_function("unwired", |b| {
        b.iter(|| black_box(service_unwired.select_write_set().expect("placement must succeed")));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_round_robin,
    bench_free_space,
    bench_upgrade_gate,
);
criterion_main!(benches);
