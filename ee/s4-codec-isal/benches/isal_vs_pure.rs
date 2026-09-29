// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Side-by-side comparison of the two codec engines.
//!
//! The baseline in `ee/s4-codec/benches/codec_bench.rs` measures the portable
//! engine alone. This one runs both engines over the same payload in the same
//! process, so the ratio between them does not depend on two runs happening
//! under the same conditions:
//!
//! ```text
//! cargo bench -p s4-codec-isal
//! ```
//!
//! Chunk size, payload and seed match the baseline file, so the absolute
//! numbers stay comparable with the ones recorded in decision EC2-007.
//!
//! # Why only the Reed-Solomon profiles
//!
//! There is no accelerated LRC engine, by decision EC2-005: LRC local parity is
//! plain XOR and already runs at memory speed, so an "accelerated" LRC profile
//! would be the portable engine measured twice. The three RS profiles are the
//! whole of what ISA-L changes.
//!
//! # What is deliberately not measured
//!
//! Codec construction. It happens once per chunk in the EC runtime, and at
//! 2-10 microseconds against 13-15 milliseconds of encoding it is under 0.1% of
//! the work. Including it would hide the thing being compared.

// `criterion_group!` expands to a public function we cannot attach docs to, and
// this crate warns on undocumented public items.
#![allow(missing_docs)]

#[cfg(target_arch = "x86_64")]
mod comparison {
    use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
    use s4_codec::reference::{split_data_shards, systematic_generator_matrix};
    use s4_codec::{build_codec, CodecEngine, EcParity, EcProfileSnapshot, EncodedChunk, Shard};
    use s4_codec_isal::ffi::{encode_data_with, init_tables_for, IsalVariant};
    use s4_codec_isal::IsalReedSolomonCodec;

    /// Payload size for one benchmarked chunk: the production default.
    const CHUNK_SIZE: usize = 6 * 1024 * 1024;

    /// Same seed as the baseline benchmark, so both files encode the same bytes.
    const PAYLOAD_SEED: u64 = 0x5445_5354;

    /// Builds repeatable pseudo-random bytes (xorshift64, fixed seed).
    ///
    /// Copied from the baseline benchmark rather than shared: `s4-codec` must not
    /// depend on this crate, not even for test code, and a dozen lines of
    /// generator is a smaller price than the dependency edge.
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

    /// The three Reed-Solomon profiles S4 ships.
    fn reed_solomon_profiles() -> Vec<EcProfileSnapshot> {
        vec![
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
        ]
    }

    /// Both engines for one profile, labelled the way the results are read.
    fn engines(profile: &EcProfileSnapshot) -> Vec<(&'static str, Box<dyn CodecEngine>)> {
        let accelerated =
            IsalReedSolomonCodec::new(profile.clone()).expect("RS profile builds an ISA-L codec");
        let portable = build_codec(profile.clone()).expect("RS profile builds a portable codec");
        vec![
            ("pure", portable),
            ("isal", Box::new(accelerated) as Box<dyn CodecEngine>),
        ]
    }

    /// Turns an encoded chunk into the `Option` slice the decode API expects,
    /// leaving the listed slots empty.
    fn shards_without(
        encoded: &EncodedChunk,
        missing: &[u8],
        total_shards: u8,
    ) -> Vec<Option<Shard>> {
        let mut slots: Vec<Option<Shard>> = vec![None; usize::from(total_shards)];
        for shard in &encoded.shards {
            if missing.contains(&shard.slot_index) {
                continue;
            }
            slots[usize::from(shard.slot_index)] = Some(shard.clone());
        }
        slots
    }

    fn bench_encode(c: &mut Criterion) {
        let payload = deterministic_payload(CHUNK_SIZE, PAYLOAD_SEED);
        let mut group = c.benchmark_group("engine/encode");
        group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));
        // One 6 MiB encode is slow enough that criterion's default sample count
        // would stretch the suite over many minutes.
        group.sample_size(20);

        for profile in reed_solomon_profiles() {
            let name = profile.name.clone();
            for (engine, codec) in engines(&profile) {
                group.bench_function(BenchmarkId::new(engine, &name), |b| {
                    b.iter(|| codec.encode_chunk(&payload, 0).expect("encode must succeed"));
                });
            }
        }
        group.finish();
    }

    /// Degraded reads only: with every data shard present the decode matrix is
    /// the identity and both engines just copy bytes. Losing `m` shards is what
    /// forces the matrix inversion and the field multiplication.
    fn bench_decode_degraded(c: &mut Criterion) {
        let payload = deterministic_payload(CHUNK_SIZE, PAYLOAD_SEED);
        let mut group = c.benchmark_group("engine/decode-degraded");
        group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));
        group.sample_size(20);

        for profile in reed_solomon_profiles() {
            let name = profile.name.clone();
            let total_shards = profile.total_shards();
            let original_size = payload.len() as u64;
            let EcParity::ReedSolomon { m } = profile.parity else {
                panic!("only Reed-Solomon profiles are compared here");
            };
            // Losing all `m` parity-recoverable slots is the heaviest decode the
            // profile can still serve.
            let losses: Vec<u8> = (0..m).collect();

            for (engine, codec) in engines(&profile) {
                let encoded = codec.encode_chunk(&payload, 0).expect("encode must succeed");
                let degraded = shards_without(&encoded, &losses, total_shards);
                group.bench_function(BenchmarkId::new(engine, &name), |b| {
                    b.iter(|| {
                        codec
                            .decode_chunk(&degraded, original_size)
                            .expect("degraded decode must succeed")
                    });
                });
            }
        }
        group.finish();
    }

    /// Rebuilding data shard 0, the case the repair worker hits most often.
    fn bench_repair(c: &mut Criterion) {
        let payload = deterministic_payload(CHUNK_SIZE, PAYLOAD_SEED);
        let mut group = c.benchmark_group("engine/repair");
        group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));
        group.sample_size(20);

        for profile in reed_solomon_profiles() {
            let name = profile.name.clone();
            let total_shards = profile.total_shards();

            for (engine, codec) in engines(&profile) {
                let encoded = codec.encode_chunk(&payload, 0).expect("encode must succeed");
                let available = shards_without(&encoded, &[0], total_shards);
                group.bench_function(BenchmarkId::new(engine, &name), |b| {
                    b.iter(|| codec.repair_shard(&available, 0).expect("repair must succeed"));
                });
            }
        }
        group.finish();
    }

    /// Cost of the parity computation alone, one ISA-L implementation at a time.
    ///
    /// Two things this answers that the engine groups above cannot:
    ///
    /// 1. **What an older CPU would do.** ISA-L picks its implementation from
    ///    CPUID, so a run only ever times the one this machine selected. Calling
    ///    the AVX2 or SSE implementation directly gives a usable estimate for a
    ///    machine without AVX-512 without needing that machine — with the caveat
    ///    that only the instructions change here, not the memory subsystem.
    /// 2. **How much of an encode is still arithmetic.** `encode_chunk` also
    ///    hashes every shard with SHA-256 and CRC32. Once the field math is
    ///    several times faster, hashing is no longer a rounding error, and the
    ///    gap between this group and `engine/encode` is what it costs.
    fn bench_parity_by_variant(c: &mut Criterion) {
        let payload = deterministic_payload(CHUNK_SIZE, PAYLOAD_SEED);
        let mut group = c.benchmark_group("variant/parity");
        group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));
        group.sample_size(20);

        for profile in reed_solomon_profiles() {
            let k = usize::from(profile.k);
            let rows = usize::from(profile.total_shards()) - k;
            let generator = systematic_generator_matrix(profile.k, profile.total_shards())
                .expect("standard profile has a generator matrix");
            let parity_rows: Vec<u8> = generator.iter().skip(k).flatten().copied().collect();
            let data_shards =
                split_data_shards(&payload, profile.k).expect("payload splits into data shards");
            let shard_len = data_shards[0].len();
            let data: Vec<&[u8]> = data_shards.iter().map(Vec::as_slice).collect();

            for variant in IsalVariant::ALL {
                if !variant.is_supported() {
                    continue;
                }
                let tables = init_tables_for(variant, k, rows, &parity_rows).expect("tables build");
                let mut parity = vec![vec![0u8; shard_len]; rows];
                group.bench_function(BenchmarkId::new(variant.name(), &profile.name), |b| {
                    b.iter(|| {
                        encode_data_with(variant, &tables, &data, &mut parity)
                            .expect("encode must succeed")
                    });
                });
            }
        }
        group.finish();
    }

    criterion_group!(
        benches,
        bench_encode,
        bench_decode_degraded,
        bench_repair,
        bench_parity_by_variant
    );
}

#[cfg(target_arch = "x86_64")]
criterion::criterion_main!(comparison::benches);

/// Off x86_64 there is no accelerated engine to compare against, and the crate
/// exposes no codec at all. Exit quietly instead of failing the build.
#[cfg(not(target_arch = "x86_64"))]
fn main() {
    println!("ISA-L is built for x86_64 only; there is nothing to compare on this target.");
}
