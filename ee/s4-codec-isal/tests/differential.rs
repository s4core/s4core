// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Differential tests: the ISA-L engine must produce exactly what the portable
//! engine produces.
//!
//! This is the acceptance criterion for the accelerated codec. A shard that
//! differs by one byte would make a mixed cluster call healthy data corrupt,
//! repair it into something else, and hand the result back to the client as
//! valid. So the comparison is over whole shards — bytes, CRC32, SHA-256, size,
//! role and chunk index — and not over decoded output alone.
//!
//! The tests only use the public API of `s4-codec-isal`, which also keeps the
//! crate honest about what it needs to expose.
//!
//! Coverage of the six standard profiles is split in two:
//!
//! - the three RS profiles are compared engine against engine, end to end;
//! - the three LRC profiles are compared through the RS geometry their global
//!   parity is built from, because decision EC2-005 leaves LRC's local XOR on
//!   the portable engine and no `IsalLrcCodec` exists.

// The accelerated engine only exists on x86_64; elsewhere there is nothing to
// compare against and the crate exports no codec at all.
#![cfg(target_arch = "x86_64")]

use s4_codec::reference::{split_data_shards, systematic_generator_matrix};
use s4_codec::{
    CodecEngine, EcParity, EcProfileSnapshot, EncodedChunk, LrcCodec, ReedSolomonCodec, Shard,
};
use s4_codec_isal::ffi::{encode_data_with, init_tables_for, IsalVariant};
use s4_codec_isal::IsalReedSolomonCodec;

/// Chunk index used everywhere; it travels into every shard, so a fixed value
/// keeps failures about the bytes rather than about bookkeeping.
const CHUNK_INDEX: u32 = 42;

/// Deterministic pseudo-random bytes.
///
/// Real payloads are not runs of zeros, and neither is this: a constant input
/// would hide a coefficient that is wrong but symmetric. The generator is a
/// xorshift so the data is reproducible without a dependency.
fn payload(size: usize) -> Vec<u8> {
    let mut state: u64 = 0x2545_f491_4f6c_dd1d;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

/// Input sizes where the split into data shards behaves differently.
fn boundary_sizes(profile: &EcProfileSnapshot) -> Vec<usize> {
    let chunk = usize::try_from(profile.chunk_size).expect("chunk size fits in usize");
    vec![
        // Empty input: k shards of a single padding byte.
        0,
        // One byte: all of it in the first shard, the rest is padding.
        1,
        // Fewer bytes than shards.
        usize::from(profile.k) - 1,
        // Odd size, so never a multiple of k for any standard profile.
        65_537,
        // Last byte short of a full chunk, and a full chunk.
        chunk - 1,
        chunk,
    ]
}

/// Every way of losing up to `max_missing` of `total` shards.
fn loss_patterns(total: u8, max_missing: u32) -> Vec<Vec<u8>> {
    (0u32..(1 << total))
        .filter(|mask| mask.count_ones() <= max_missing)
        .map(|mask| (0..total).filter(|slot| mask & (1 << slot) != 0).collect())
        .collect()
}

fn shards_without(encoded: &EncodedChunk, missing: &[u8]) -> Vec<Option<Shard>> {
    encoded
        .shards
        .iter()
        .map(|shard| {
            if missing.contains(&shard.slot_index) {
                None
            } else {
                Some(shard.clone())
            }
        })
        .collect()
}

fn engines(profile: &EcProfileSnapshot) -> (ReedSolomonCodec, IsalReedSolomonCodec) {
    (
        ReedSolomonCodec::new(profile.clone()).expect("portable engine accepts the profile"),
        IsalReedSolomonCodec::new(profile.clone()).expect("ISA-L engine accepts the profile"),
    )
}

/// Compares both engines on one profile and one input size.
fn assert_encode_matches(profile: &EcProfileSnapshot, size: usize) {
    let (pure, isal) = engines(profile);
    let input = payload(size);

    let expected = pure.encode_chunk(&input, CHUNK_INDEX).expect("portable encode");
    let actual = isal.encode_chunk(&input, CHUNK_INDEX).expect("ISA-L encode");

    // Shard by shard first: a whole-struct assertion would print two megabytes
    // of bytes and not say which slot went wrong.
    for (expected_shard, actual_shard) in expected.shards.iter().zip(&actual.shards) {
        assert_eq!(
            expected_shard.data, actual_shard.data,
            "profile {}, {size} byte(s): slot {} differs",
            profile.name, expected_shard.slot_index
        );
        assert_eq!(expected_shard, actual_shard);
    }
    assert_eq!(expected, actual, "profile {}, {size} byte(s)", profile.name);

    // Identical shards mean either engine can read the other's output. Proving
    // it here costs one decode and covers the case the whole plan exists for.
    let all_present = shards_without(&actual, &[]);
    let decoded = pure.decode_chunk(&all_present, actual.original_size).expect("portable decode");
    assert_eq!(decoded.as_ref(), input.as_slice());
    let decoded = isal
        .decode_chunk(&shards_without(&expected, &[]), expected.original_size)
        .unwrap();
    assert_eq!(decoded.as_ref(), input.as_slice());
}

/// One test per profile: they run in parallel, and the full-chunk cases are the
/// slow part.
#[test]
fn encode_is_byte_identical_on_ec_rs_small() {
    let profile = EcProfileSnapshot::rs_small();
    for size in boundary_sizes(&profile) {
        assert_encode_matches(&profile, size);
    }
}

#[test]
fn encode_is_byte_identical_on_ec_rs_standard() {
    let profile = EcProfileSnapshot::rs_standard();
    for size in boundary_sizes(&profile) {
        assert_encode_matches(&profile, size);
    }
}

#[test]
fn encode_is_byte_identical_on_ec_rs_dense() {
    let profile = EcProfileSnapshot::rs_dense();
    for size in boundary_sizes(&profile) {
        assert_encode_matches(&profile, size);
    }
}

/// Decoding must agree on every loss pattern the profile can survive, and fail
/// on the same ones it cannot.
#[test]
fn decode_is_byte_identical_on_every_loss_pattern() {
    for profile in [
        EcProfileSnapshot::rs_small(),
        EcProfileSnapshot::rs_standard(),
        EcProfileSnapshot::rs_dense(),
    ] {
        let (pure, isal) = engines(&profile);
        let input = payload(9_973);
        let encoded = pure.encode_chunk(&input, CHUNK_INDEX).expect("portable encode");
        let parity = u32::from(profile.total_shards() - profile.k);

        for missing in loss_patterns(profile.total_shards(), parity + 1) {
            let shards = shards_without(&encoded, &missing);
            let expected = pure.decode_chunk(&shards, encoded.original_size);
            let actual = isal.decode_chunk(&shards, encoded.original_size);

            match (expected, actual) {
                (Ok(expected), Ok(actual)) => {
                    assert_eq!(expected.as_ref(), input.as_slice());
                    assert_eq!(
                        expected, actual,
                        "profile {}, missing {missing:?}",
                        profile.name
                    );
                }
                (Err(expected), Err(actual)) => assert_eq!(
                    expected.to_string(),
                    actual.to_string(),
                    "profile {}, missing {missing:?}",
                    profile.name
                ),
                (expected, actual) => panic!(
                    "profile {}, missing {missing:?}: engines disagree, portable gave {expected:?} \
                     and ISA-L gave {actual:?}",
                    profile.name
                ),
            }
        }
    }
}

/// A repaired shard has to match the original one completely, including the
/// hashes the scrubber compares against the manifest.
#[test]
fn repair_is_byte_identical_on_every_slot() {
    for profile in [
        EcProfileSnapshot::rs_small(),
        EcProfileSnapshot::rs_standard(),
        EcProfileSnapshot::rs_dense(),
    ] {
        let (pure, isal) = engines(&profile);
        let input = payload(20_011);
        let encoded = pure.encode_chunk(&input, CHUNK_INDEX).expect("portable encode");
        let parity = profile.total_shards() - profile.k;

        for missing_slot in 0..profile.total_shards() {
            // Repair the slot alone, then repair it while the profile is at the
            // edge of its budget: the second case forces a matrix inversion
            // over a different set of rows.
            let mut companions = (0..profile.total_shards())
                .filter(|slot| *slot != missing_slot)
                .take(usize::from(parity) - 1)
                .collect::<Vec<_>>();
            companions.push(missing_slot);

            for missing in [vec![missing_slot], companions] {
                let shards = shards_without(&encoded, &missing);
                let expected = pure.repair_shard(&shards, missing_slot).expect("portable repair");
                let actual = isal.repair_shard(&shards, missing_slot).expect("ISA-L repair");

                assert_eq!(
                    expected, actual,
                    "profile {}, repairing slot {missing_slot} with {missing:?} missing",
                    profile.name
                );
                assert_eq!(expected, encoded.shards[usize::from(missing_slot)]);
            }
        }
    }
}

/// Compares degraded decode and repair on one profile at every boundary size.
///
/// The loss-pattern and repair tests above run on payloads of a few kilobytes,
/// where every shard is comfortably larger than a SIMD block. A tiny object
/// produces one-byte shards, and ISA-L answers those from its portable path
/// instead of the assembly one — a different code path, and therefore worth
/// comparing rather than assuming.
fn assert_degraded_paths_match(profile: &EcProfileSnapshot) {
    /// Above this, only decoding is compared: repairing every lost slot of a
    /// full chunk in a debug build costs minutes and proves nothing new.
    const REPAIR_LIMIT: usize = 1 << 20;

    let (pure, isal) = engines(profile);
    let parity = profile.total_shards() - profile.k;

    for size in boundary_sizes(profile) {
        let input = payload(size);
        let encoded = pure.encode_chunk(&input, CHUNK_INDEX).expect("portable encode");

        // Lose as much as the profile tolerates, taken from the front, so the
        // surviving rows are not the identity block and decoding has to invert
        // a real matrix.
        let missing = (0..parity).collect::<Vec<_>>();
        let shards = shards_without(&encoded, &missing);

        let expected = pure.decode_chunk(&shards, encoded.original_size).expect("portable decode");
        let actual = isal.decode_chunk(&shards, encoded.original_size).expect("ISA-L decode");
        assert_eq!(expected.as_ref(), input.as_slice());
        assert_eq!(
            expected, actual,
            "profile {}, {size} byte(s), missing {missing:?}",
            profile.name
        );

        if size > REPAIR_LIMIT {
            continue;
        }
        for missing_slot in missing {
            let expected = pure.repair_shard(&shards, missing_slot).expect("portable repair");
            let actual = isal.repair_shard(&shards, missing_slot).expect("ISA-L repair");
            assert_eq!(
                expected, actual,
                "profile {}, {size} byte(s), repairing slot {missing_slot}",
                profile.name
            );
            assert_eq!(expected, encoded.shards[usize::from(missing_slot)]);
        }
    }
}

#[test]
fn degraded_paths_are_byte_identical_on_ec_rs_small() {
    assert_degraded_paths_match(&EcProfileSnapshot::rs_small());
}

#[test]
fn degraded_paths_are_byte_identical_on_ec_rs_standard() {
    assert_degraded_paths_match(&EcProfileSnapshot::rs_standard());
}

#[test]
fn degraded_paths_are_byte_identical_on_ec_rs_dense() {
    assert_degraded_paths_match(&EcProfileSnapshot::rs_dense());
}

/// Operators and tests read this table, so both engines must publish the same
/// one, entry for entry and in the same order.
#[test]
fn failure_matrix_is_identical_on_every_rs_profile() {
    for profile in [
        EcProfileSnapshot::rs_small(),
        EcProfileSnapshot::rs_standard(),
        EcProfileSnapshot::rs_dense(),
    ] {
        let (pure, isal) = engines(&profile);
        assert_eq!(pure.failure_matrix(), isal.failure_matrix());
    }
}

/// The LRC profiles reach the matrix through their global parity.
///
/// `LrcCodec` computes global parity with RS over `k` data shards and `global`
/// parity shards. That is the same geometry, and therefore the same generator
/// matrix, as an RS profile with `m = global`. Comparing the two proves that
/// data written under an LRC profile stays readable in a cluster where the RS
/// math is accelerated.
#[test]
fn lrc_global_parity_matches_the_accelerated_rs_geometry() {
    for profile in [
        EcProfileSnapshot::lrc_small(),
        EcProfileSnapshot::lrc_standard(),
        EcProfileSnapshot::lrc_dense(),
    ] {
        let EcParity::Lrc { local, global } = profile.parity else {
            panic!("{} must be an LRC profile", profile.name);
        };
        let lrc = LrcCodec::new(profile.clone()).expect("portable LRC engine");
        let rs_geometry =
            EcProfileSnapshot::new_rs(format!("{}-global", profile.name), profile.k, global)
                .expect("global parity forms a valid RS profile");
        let isal = IsalReedSolomonCodec::new(rs_geometry).expect("ISA-L engine");

        let input = payload(70_001);
        let expected = lrc.encode_chunk(&input, CHUNK_INDEX).expect("LRC encode");
        let actual = isal.encode_chunk(&input, CHUNK_INDEX).expect("ISA-L encode");

        for slot in 0..profile.k {
            assert_eq!(
                expected.shards[usize::from(slot)].data,
                actual.shards[usize::from(slot)].data,
                "profile {}: data slot {slot} differs",
                profile.name
            );
        }
        for index in 0..global {
            let lrc_slot = profile.k + local + index;
            let rs_slot = profile.k + index;
            assert_eq!(
                expected.shards[usize::from(lrc_slot)].data,
                actual.shards[usize::from(rs_slot)].data,
                "profile {}: global parity {index} differs",
                profile.name
            );
        }
    }
}

/// Computes the parity of one chunk with every ISA-L implementation this CPU
/// can run, and compares each against the portable engine.
///
/// Returns the names of the implementations that were exercised, so the caller
/// can tell an honest pass from a pass where nothing ran.
fn assert_every_variant_matches(profile: &EcProfileSnapshot, size: usize) -> Vec<&'static str> {
    let pure = ReedSolomonCodec::new(profile.clone()).expect("portable engine accepts the profile");
    let input = payload(size);
    let expected = pure.encode_chunk(&input, CHUNK_INDEX).expect("portable encode");

    // The same inputs `IsalReedSolomonCodec` builds internally: the parity rows
    // of the S4 generator matrix, and the chunk split into data shards.
    let k = usize::from(profile.k);
    let rows = usize::from(profile.total_shards()) - k;
    let generator = systematic_generator_matrix(profile.k, profile.total_shards())
        .expect("standard profile has a generator matrix");
    let parity_rows: Vec<u8> = generator.iter().skip(k).flatten().copied().collect();
    let data_shards = split_data_shards(&input, profile.k).expect("input splits into data shards");
    let shard_len = data_shards[0].len();
    let data: Vec<&[u8]> = data_shards.iter().map(Vec::as_slice).collect();

    let mut covered = Vec::new();
    for variant in IsalVariant::ALL {
        if !variant.is_supported() {
            continue;
        }

        let tables = init_tables_for(variant, k, rows, &parity_rows).expect("tables build");
        let mut parity = vec![vec![0u8; shard_len]; rows];
        encode_data_with(variant, &tables, &data, &mut parity).expect("encode succeeds");

        for (index, computed) in parity.iter().enumerate() {
            let slot = k + index;
            assert_eq!(
                computed.as_slice(),
                expected.shards[slot].data.as_ref(),
                "profile {}, {size} byte(s): ISA-L implementation '{}' disagrees with the \
                 portable engine on slot {slot}",
                profile.name,
                variant.name()
            );
        }
        covered.push(variant.name());
    }

    covered
}

/// Byte equality has to hold for every implementation, not just the one this
/// machine happens to select.
///
/// `ec_encode_data` is a dispatcher: it reads CPUID once and jumps to one of
/// seven implementations — portable C, SSE, AVX, AVX2, AVX2+GFNI, AVX-512 and
/// AVX-512+GFNI. Every other test in this file goes through that dispatcher, so
/// on its own each one proves equality for a single implementation and assumes
/// it for the rest. A cluster mixing an old and a new machine would be running
/// two implementations that were never compared against each other.
///
/// Calling each one directly turns that assumption into a check: one machine
/// covers everything it can execute, and machines of different generations
/// cover the remainder between them. See task `EC2-ISA-04-003`.
#[test]
fn every_dispatch_variant_matches_the_portable_engine() {
    let mut covered: Option<Vec<&'static str>> = None;

    for profile in [
        EcProfileSnapshot::rs_small(),
        EcProfileSnapshot::rs_standard(),
        EcProfileSnapshot::rs_dense(),
    ] {
        // One byte makes every shard shorter than a SIMD block, which sends the
        // assembly implementations down their short-buffer path; the other two
        // sizes exercise the vectorised one, with and without a remainder.
        for size in [1, 65_537, 1 << 20] {
            let exercised = assert_every_variant_matches(&profile, size);
            match &covered {
                None => covered = Some(exercised),
                Some(first) => assert_eq!(
                    first, &exercised,
                    "CPU support must not change between cases"
                ),
            }
        }
    }

    let covered = covered.expect("at least one profile was compared");
    println!(
        "ISA-L implementations compared on this CPU: {}",
        covered.join(", ")
    );

    // The dispatcher and the portable C path run on any x86_64 CPU. If even
    // those are missing, the loop above compared nothing and passed silently.
    assert!(
        covered.contains(&"dispatched") && covered.contains(&"base"),
        "expected at least the dispatcher and the base implementation, got {covered:?}"
    );
}

/// Refusals must match too: a caller that inspects the error has to see the
/// same thing from either engine.
#[test]
fn both_engines_refuse_the_same_inputs() {
    let profile = EcProfileSnapshot::rs_small();
    let (pure, isal) = engines(&profile);
    let encoded = pure.encode_chunk(&payload(4_096), CHUNK_INDEX).expect("portable encode");

    // Too few shards left to solve for the data.
    let shards = shards_without(&encoded, &[0, 1, 2]);
    assert_eq!(
        pure.decode_chunk(&shards, encoded.original_size).unwrap_err().to_string(),
        isal.decode_chunk(&shards, encoded.original_size).unwrap_err().to_string()
    );

    // A shard whose checksum no longer matches its bytes.
    let mut shards = shards_without(&encoded, &[]);
    shards[2].as_mut().expect("slot 2 is present").sha256 = [0; 32];
    assert_eq!(
        pure.decode_chunk(&shards, encoded.original_size).unwrap_err().to_string(),
        isal.decode_chunk(&shards, encoded.original_size).unwrap_err().to_string()
    );

    // Repairing a slot that is not missing, and one outside the profile.
    let shards = shards_without(&encoded, &[]);
    assert_eq!(
        pure.repair_shard(&shards, 1).unwrap_err().to_string(),
        isal.repair_shard(&shards, 1).unwrap_err().to_string()
    );
    assert_eq!(
        pure.repair_shard(&shards, profile.total_shards()).unwrap_err().to_string(),
        isal.repair_shard(&shards, profile.total_shards()).unwrap_err().to_string()
    );
}
