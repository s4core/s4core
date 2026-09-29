// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Pure-Rust codec baseline for EC v2 plan 01 (ISA-L FFI).
//!
//! These numbers are the reference the SIMD codec is compared against. Run them
//! before touching the codec, and keep the result in the repository:
//!
//! ```text
//! cargo bench -p s4-codec
//! ```
//!
//! Three operations are measured, because they have different cost profiles:
//!
//! * `encode`  - the write path. Every stored byte goes through it.
//! * `decode`  - the read path. Measured twice, see below.
//! * `repair`  - rebuilding one lost shard, used by the repair worker.
//!
//! Decode is measured in two variants on purpose. When all data shards are
//! present the decode matrix is the identity, so the codec only copies bytes and
//! the result says nothing about the finite-field math. The degraded variant
//! removes data shards and forces the codec to invert a matrix and multiply, and
//! that is the number worth comparing against ISA-L.

// `criterion_group!` expands to a public function we cannot attach docs to, and
// the workspace warns on undocumented public items.
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use s4_codec::{build_codec, EcProfileSnapshot, EncodedChunk, Shard};

/// Payload size for one benchmarked chunk.
///
/// This matches the production default (`DEFAULT_CHUNK_SIZE`, 6 MiB) so the
/// baseline describes real behaviour rather than a cache-friendly toy size.
const CHUNK_SIZE: usize = 6 * 1024 * 1024;

/// Builds repeatable pseudo-random bytes.
///
/// A fixed seed keeps runs comparable over time. A real random source would make
/// every run measure slightly different data, which is not what we want from a
/// baseline. This is not a cryptographic generator and does not need to be one.
fn deterministic_payload(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        // xorshift64: cheap, dependency free, and good enough to avoid long runs
        // of equal bytes that would make the codec look faster than it is.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// The six profiles S4 ships. Every codec change must hold for all of them.
fn all_profiles() -> Vec<EcProfileSnapshot> {
    vec![
        EcProfileSnapshot::rs_small(),
        EcProfileSnapshot::rs_standard(),
        EcProfileSnapshot::rs_dense(),
        EcProfileSnapshot::lrc_small(),
        EcProfileSnapshot::lrc_standard(),
        EcProfileSnapshot::lrc_dense(),
    ]
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

/// Picks a loss pattern that every profile can still recover from.
///
/// For RS this is any `m` shards. For LRC a single loss is always recoverable by
/// the local group, so we take one shard to stay inside the documented matrix
/// instead of guessing at what the two-stage recovery can handle.
fn recoverable_losses(profile: &EcProfileSnapshot) -> Vec<u8> {
    match profile.parity {
        s4_codec::EcParity::ReedSolomon { m } => (0..m).collect(),
        s4_codec::EcParity::Lrc { .. } => vec![0],
    }
}

fn bench_encode(c: &mut Criterion) {
    let payload = deterministic_payload(CHUNK_SIZE, 0x5445_5354);
    let mut group = c.benchmark_group("encode");
    group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));
    // A single 6 MiB encode takes long enough that criterion's default sample
    // count would make the suite run for many minutes.
    group.sample_size(20);

    for profile in all_profiles() {
        let name = profile.name.clone();
        let codec = build_codec(profile).expect("profile must build a codec");
        group.bench_function(BenchmarkId::from_parameter(&name), |b| {
            b.iter(|| codec.encode_chunk(&payload, 0).expect("encode must succeed"));
        });
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let payload = deterministic_payload(CHUNK_SIZE, 0x5445_5354);
    let mut group = c.benchmark_group("decode");
    group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));
    group.sample_size(20);

    for profile in all_profiles() {
        let name = profile.name.clone();
        let total_shards = profile.total_shards();
        let original_size = payload.len() as u64;
        let losses = recoverable_losses(&profile);
        let codec = build_codec(profile).expect("profile must build a codec");
        let encoded = codec.encode_chunk(&payload, 0).expect("encode must succeed");

        // Nothing lost: the decode matrix is the identity and this mostly
        // measures memory bandwidth. Kept as a floor to compare against.
        let intact = shards_without(&encoded, &[], total_shards);
        group.bench_function(BenchmarkId::new("intact", &name), |b| {
            b.iter(|| codec.decode_chunk(&intact, original_size).expect("decode must succeed"));
        });

        // Data shards missing: forces matrix inversion and field multiplication.
        // This is the number that matters for the ISA-L comparison.
        let degraded = shards_without(&encoded, &losses, total_shards);
        group.bench_function(BenchmarkId::new("degraded", &name), |b| {
            b.iter(|| {
                codec
                    .decode_chunk(&degraded, original_size)
                    .expect("degraded decode must succeed")
            });
        });
    }
    group.finish();
}

fn bench_repair(c: &mut Criterion) {
    let payload = deterministic_payload(CHUNK_SIZE, 0x5445_5354);
    let mut group = c.benchmark_group("repair");
    group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));
    group.sample_size(20);

    for profile in all_profiles() {
        let name = profile.name.clone();
        let total_shards = profile.total_shards();
        let codec = build_codec(profile).expect("profile must build a codec");
        let encoded = codec.encode_chunk(&payload, 0).expect("encode must succeed");

        // Rebuild data shard 0, the case the repair worker hits most often.
        let available = shards_without(&encoded, &[0], total_shards);
        group.bench_function(BenchmarkId::from_parameter(&name), |b| {
            b.iter(|| codec.repair_shard(&available, 0).expect("repair must succeed"));
        });
    }
    group.finish();
}

/// The part of an encode that is not field arithmetic.
///
/// Splitting a chunk into `k` data shards is portable code, identical on both
/// engines, and it runs before any parity is computed. On the pure-Rust engine
/// it is a small share of the encode and easy to overlook. Once ISA-L makes the
/// arithmetic almost free, it stops being small, so it is measured separately
/// instead of being inferred from the difference between two other numbers.
fn bench_split(c: &mut Criterion) {
    let payload = deterministic_payload(CHUNK_SIZE, 0x5445_5354);
    let mut group = c.benchmark_group("split");
    group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));
    group.sample_size(20);

    for profile in all_profiles() {
        let name = profile.name.clone();
        let k = profile.k;
        group.bench_function(BenchmarkId::from_parameter(&name), |b| {
            b.iter(|| s4_codec::reference::split_data_shards(&payload, k).expect("split"));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_encode,
    bench_decode,
    bench_repair,
    bench_split
);
criterion_main!(benches);
