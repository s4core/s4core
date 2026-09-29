// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! What each LRC recovery route costs, for EC v2 plan 06 (`EC2-LRC-05-001`).
//!
//! ```text
//! cargo bench -p s4-codec --bench lrc_bench
//! ```
//!
//! An LRC chunk comes back by one of three routes, and they are not
//! alternatives an operator picks between — the decoder takes the cheapest one
//! that reaches the loss it is looking at:
//!
//! * **local XOR** — one data shard missing from a group whose local parity
//!   survives. `k / local` reads, one pass of XOR, no cross-zone traffic.
//! * **global Reed-Solomon** — the EC v1 fallback. Reads `k` shards and
//!   inverts a `k × k` matrix, and never looks at local parity.
//! * **unified solver** — plan 06. Puts every surviving row, local parity
//!   included, into one system and solves it. It is the only route to a group
//!   that lost more data shards than global parity alone carries.
//!
//! So the question these benchmarks answer is not "is the solver faster" — it
//! is reached only where nothing else reaches at all. The question is whether
//! a degraded read that falls to it stays in the same order of magnitude as
//! one the fast paths serve. Every pattern below is checked against
//! `recovery_path` before it is timed, so a benchmark cannot quietly measure a
//! route other than the one it is named after.

// `criterion_group!` expands to a public function we cannot attach docs to, and
// the workspace warns on undocumented public items.
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use s4_codec::{
    build_codec, CodecEngine, EcParity, EcProfileSnapshot, EncodedChunk, RecoveryPath, Shard,
};

/// Payload size for one benchmarked chunk, the production default (6 MiB).
const CHUNK_SIZE: usize = 6 * 1024 * 1024;

/// Builds repeatable pseudo-random bytes.
///
/// A fixed seed keeps runs comparable over time; this is not a cryptographic
/// generator and does not need to be one.
fn deterministic_payload(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// The three LRC profiles S4 ships.
fn lrc_profiles() -> Vec<EcProfileSnapshot> {
    vec![
        EcProfileSnapshot::lrc_small(),
        EcProfileSnapshot::lrc_standard(),
        EcProfileSnapshot::lrc_dense(),
    ]
}

/// The shape of an LRC profile: data shards, local groups, global parity.
fn shape(profile: &EcProfileSnapshot) -> (u8, u8, u8) {
    match profile.parity {
        EcParity::Lrc { local, global } => (profile.k, local, global),
        EcParity::ReedSolomon { .. } => panic!("{} is not an LRC profile", profile.name),
    }
}

/// One loss pattern, named after the route the decoder takes to get past it.
struct Route {
    /// Benchmark id, and the route the pattern is expected to take.
    name: &'static str,
    /// Slots whose shards are gone.
    missing: Vec<u8>,
    /// The route `recovery_path` must agree this pattern takes.
    path: RecoveryPath,
}

/// The loss patterns worth timing, one per route, for one profile.
///
/// * `direct` — nothing is missing. The decode is a copy, and it is here as the
///   floor every other number is read against.
/// * `local-xor` — one data shard of group 0, its local parity alive.
/// * `global-rs` — the same data shard *and* its group's local parity, which
///   shuts the local stage out and leaves the loss to Reed-Solomon.
/// * `unified-solver` — one more data shard of group 0 than global parity
///   carries, local parity alive. Neither fast path reaches it.
fn routes(profile: &EcProfileSnapshot) -> Vec<Route> {
    let (k, local, global) = shape(profile);
    let mut routes = vec![
        Route {
            name: "direct",
            missing: Vec::new(),
            path: RecoveryPath::Direct,
        },
        Route {
            name: "local-xor",
            missing: vec![0],
            path: RecoveryPath::LocalXor,
        },
        Route {
            name: "global-rs",
            missing: vec![0, k],
            path: RecoveryPath::GlobalRs,
        },
    ];
    if k / local > global {
        routes.push(Route {
            name: "unified-solver",
            missing: (0..=global).collect(),
            path: RecoveryPath::UnifiedSolver,
        });
    }
    routes
}

/// Turns an encoded chunk into the `Option` slice the decode API expects,
/// leaving the listed slots empty.
fn shards_without(encoded: &EncodedChunk, missing: &[u8], total_shards: u8) -> Vec<Option<Shard>> {
    let mut slots: Vec<Option<Shard>> = vec![None; usize::from(total_shards)];
    for shard in &encoded.shards {
        if missing.contains(&shard.slot_index) {
            continue;
        }
        slots[usize::from(shard.slot_index)] = Some(shard.clone());
    }
    slots
}

/// Refuses to time a pattern that does not take the route it is named after.
///
/// Without this the suite would keep reporting numbers after a change moved a
/// pattern from one route to another, and the comparison it exists for would
/// quietly become a comparison of one route with itself.
fn assert_route(codec: &dyn CodecEngine, profile: &EcProfileSnapshot, route: &Route) {
    let lost = (0..profile.total_shards())
        .map(|slot| route.missing.contains(&slot))
        .collect::<Vec<_>>();
    assert_eq!(
        codec.recovery_path(&lost),
        route.path,
        "{} / {}: pattern {:?} does not take the route it is named after",
        profile.name,
        route.name,
        route.missing
    );
}

fn bench_decode(c: &mut Criterion) {
    let payload = deterministic_payload(CHUNK_SIZE, 0x4C52_4332);
    let mut group = c.benchmark_group("lrc_decode");
    group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));
    // A 6 MiB chunk takes long enough that criterion's default sample count
    // would make the suite run for many minutes.
    group.sample_size(20);

    for profile in lrc_profiles() {
        let codec = build_codec(profile.clone()).expect("profile must build a codec");
        let encoded = codec.encode_chunk(&payload, 0).expect("encode must succeed");

        for route in routes(&profile) {
            assert_route(codec.as_ref(), &profile, &route);
            let shards = shards_without(&encoded, &route.missing, profile.total_shards());
            let id = BenchmarkId::new(&profile.name, route.name);
            group.bench_function(id, |b| {
                b.iter(|| {
                    codec.decode_chunk(&shards, encoded.original_size).expect("decode must succeed")
                });
            });
        }
    }
    group.finish();
}

fn bench_repair(c: &mut Criterion) {
    let payload = deterministic_payload(CHUNK_SIZE, 0x4C52_4332);
    let mut group = c.benchmark_group("lrc_repair");
    // One shard, not the whole chunk, is what a repair produces.
    group.sample_size(20);

    for profile in lrc_profiles() {
        let codec = build_codec(profile.clone()).expect("profile must build a codec");
        let encoded = codec.encode_chunk(&payload, 0).expect("encode must succeed");
        group.throughput(Throughput::Bytes(encoded.shard_size));

        // Slot 0 is rebuilt twice: from its own local group while the group is
        // otherwise whole, and by the solver once the group has lost more than
        // the fast paths can carry. Same slot, same bytes out, two routes in.
        for route in routes(&profile).into_iter().filter(|route| {
            matches!(
                route.path,
                RecoveryPath::LocalXor | RecoveryPath::UnifiedSolver
            )
        }) {
            assert_route(codec.as_ref(), &profile, &route);
            let shards = shards_without(&encoded, &route.missing, profile.total_shards());
            let id = BenchmarkId::new(&profile.name, route.name);
            group.bench_function(id, |b| {
                b.iter(|| codec.repair_shard(&shards, 0).expect("repair must succeed"));
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_decode, bench_repair);
criterion_main!(benches);
