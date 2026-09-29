// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Reed-Solomon codec engine that does its field arithmetic in ISA-L.
//!
//! The engine is a drop-in replacement for `s4_codec::ReedSolomonCodec`: same
//! trait, same shards, same bytes. Only the arithmetic moves to SIMD.
//!
//! # How byte equality is kept
//!
//! Everything that decides what a shard contains comes from `s4-codec`:
//!
//! - the generator matrix, from `systematic_generator_matrix`;
//! - the split into data shards and the padding it adds;
//! - shard roles, checksums, and the trimming done when decoding.
//!
//! ISA-L is handed the parity rows of that matrix and asked to apply them. It
//! is never asked to invent one — its `gf_gen_rs_matrix` and
//! `gf_gen_cauchy1_matrix` build different coefficients and are therefore not
//! declared in [`crate::ffi`] at all. See decision EC2-002.
//!
//! LRC profiles are out of scope, per decision EC2-005: their local parity is
//! plain XOR, which already runs at memory speed. An LRC profile is rejected
//! here rather than silently mishandled.

use bytes::Bytes;
use s4_codec::reference::{
    ensure_enough_candidates, ensure_slot_in_profile, ensure_slot_is_absent, invert_matrix,
    reed_solomon_failure_matrix, reed_solomon_recovery_path, split_data_shards,
    systematic_generator_matrix, truncate_data_shards, validate_data_shards, ShardCandidate,
    ValidShardSet,
};
use s4_codec::{
    CodecEngine, CodecError, EcCodecKind, EcProfileSnapshot, EncodedChunk, FailureMatrix,
    RecoveryPath, Shard,
};

use crate::ffi;

/// Reed-Solomon codec engine accelerated with ISA-L.
#[derive(Debug, Clone)]
pub struct IsalReedSolomonCodec {
    profile: EcProfileSnapshot,
    /// Systematic generator matrix: `total_shards` rows of `k` coefficients.
    generator: Vec<Vec<u8>>,
    /// ISA-L tables for the parity rows of `generator`.
    ///
    /// Built once at construction because every encode uses the same rows.
    parity_tables: Vec<u8>,
}

impl IsalReedSolomonCodec {
    /// Build an ISA-L backed Reed-Solomon codec engine.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::InvalidProfile`] when the profile is invalid or is
    /// not a Reed-Solomon profile, and [`CodecError::Arithmetic`] when the
    /// generator matrix cannot be built for its dimensions.
    pub fn new(profile: EcProfileSnapshot) -> Result<Self, CodecError> {
        profile.validate()?;
        Self::require_reed_solomon(&profile)?;

        let generator = systematic_generator_matrix(profile.k, profile.total_shards())?;
        Self::from_generator(profile, generator)
    }

    /// Builds the codec on a caller-supplied generator matrix.
    ///
    /// **Not for production.** Encoding with any matrix other than the one
    /// `s4-codec` builds is exactly the mistake decision EC2-002 forbids, and
    /// it would make this node's shards unreadable to the rest of the cluster.
    ///
    /// It exists because the mixed-cluster acceptance script has to make that
    /// mistake deliberately: a byte-equality check that has never been observed
    /// to fail proves nothing. The only caller is the EC test hook, which
    /// refuses to arm unless `S4_EC_TEST_HOOKS_ENABLED` is set.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::InvalidProfile`] when the profile is not a valid
    /// Reed-Solomon one, and [`CodecError::InvalidInput`] when the matrix does
    /// not have one row of `k` coefficients per shard slot.
    pub fn with_generator_matrix(
        profile: EcProfileSnapshot,
        generator: Vec<Vec<u8>>,
    ) -> Result<Self, CodecError> {
        profile.validate()?;
        Self::require_reed_solomon(&profile)?;
        Self::from_generator(profile, generator)
    }

    /// Shared tail of both constructors: check the matrix, build the tables.
    fn from_generator(
        profile: EcProfileSnapshot,
        generator: Vec<Vec<u8>>,
    ) -> Result<Self, CodecError> {
        // `validate` has already established k >= 1 and m >= 1, so the profile
        // has parity rows and the matrix must have more rows than k.
        let total_shards = profile.total_shards();
        let k = usize::from(profile.k);
        if generator.len() != usize::from(total_shards)
            || generator.iter().any(|row| row.len() != k)
        {
            return Err(CodecError::InvalidInput {
                reason: format!(
                    "generator matrix must be {total_shards} row(s) of {k} coefficient(s)"
                ),
            });
        }

        // The top block must be the identity. `encode_chunk` stores the split
        // data shards as they are instead of multiplying them through, so a
        // matrix whose top rows are anything else would produce shards that its
        // own decode could not invert back.
        for (row, coefficients) in generator.iter().take(k).enumerate() {
            if coefficients
                .iter()
                .enumerate()
                .any(|(column, coefficient)| *coefficient != u8::from(column == row))
            {
                return Err(CodecError::InvalidInput {
                    reason: format!(
                        "generator matrix row {row} is not the identity; \
                         only systematic matrices are supported"
                    ),
                });
            }
        }

        let parity_rows = generator.iter().skip(k).flatten().copied().collect::<Vec<u8>>();
        let parity_tables =
            ffi::init_tables(k, usize::from(total_shards - profile.k), &parity_rows)?;

        Ok(Self {
            profile,
            generator,
            parity_tables,
        })
    }

    /// Rejects profiles this engine does not implement.
    fn require_reed_solomon(profile: &EcProfileSnapshot) -> Result<(), CodecError> {
        if profile.codec != EcCodecKind::ReedSolomon {
            return Err(CodecError::InvalidProfile {
                profile: profile.name.clone(),
                reason: "IsalReedSolomonCodec requires a Reed-Solomon profile".to_string(),
            });
        }
        Ok(())
    }

    /// Computes the parity shards of one chunk.
    ///
    /// Uses the tables built at construction instead of going through
    /// [`apply_rows`], because the parity rows never change and rebuilding
    /// their tables per chunk would be wasted work on the write path.
    fn encode_parity(&self, data_shards: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, CodecError> {
        // `split_data_shards` never produces an empty shard, so unlike
        // `apply_rows` this path has no zero-length case to handle.
        let shard_size = validate_data_shards(self.profile.k, data_shards)?;
        let parity_count = usize::from(self.profile.total_shards() - self.profile.k);
        let mut parity = vec![vec![0u8; shard_size]; parity_count];
        ffi::encode_data(&self.parity_tables, &as_slices(data_shards), &mut parity)?;
        Ok(parity)
    }

    /// Rebuilds the `k` data shards from any `k` valid shards.
    ///
    /// The generator rows of the shards at hand form a square matrix; its
    /// inverse maps those shards back to the data shards.
    fn decode_data_shards(
        &self,
        candidates: &[ShardCandidate],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        ensure_enough_candidates(self.profile.k, candidates)?;

        let selected = &candidates[..usize::from(self.profile.k)];
        let matrix = selected
            .iter()
            .map(|candidate| self.generator_row(candidate.slot_index()).map(<[u8]>::to_vec))
            .collect::<Result<Vec<_>, _>>()?;
        let inverse = invert_matrix(&matrix)?;
        let sources =
            selected.iter().map(|candidate| candidate.data().as_ref()).collect::<Vec<_>>();

        apply_rows(&flatten_rows(&inverse), inverse.len(), &sources)
    }

    /// Computes the shard that belongs in one slot from the data shards.
    fn encode_slot(&self, slot: u8, data_shards: &[Vec<u8>]) -> Result<Vec<u8>, CodecError> {
        validate_data_shards(self.profile.k, data_shards)?;
        let row = self.generator_row(slot)?;
        let sources = as_slices(data_shards);
        let mut encoded = apply_rows(row, 1, &sources)?;
        encoded.pop().ok_or_else(|| CodecError::Arithmetic {
            reason: "encoding a single generator row produced no output".to_string(),
        })
    }

    /// Returns one row of the generator matrix.
    fn generator_row(&self, slot: u8) -> Result<&[u8], CodecError> {
        self.generator.get(usize::from(slot)).map(Vec::as_slice).ok_or_else(|| {
            CodecError::InvalidShardSet {
                reason: format!("slot {slot} is outside RS generator matrix"),
            }
        })
    }
}

impl CodecEngine for IsalReedSolomonCodec {
    fn profile(&self) -> &EcProfileSnapshot {
        &self.profile
    }

    fn encode_chunk(&self, input: &[u8], chunk_index: u32) -> Result<EncodedChunk, CodecError> {
        let data = split_data_shards(input, self.profile.k)?;
        let parity = self.encode_parity(&data)?;

        let shards = data
            .into_iter()
            .chain(parity)
            .enumerate()
            .map(|(slot, bytes)| {
                let slot_index = u8::try_from(slot).map_err(|_| CodecError::InvalidInput {
                    reason: "slot index does not fit in u8".to_string(),
                })?;
                Shard::new(
                    slot_index,
                    chunk_index,
                    self.profile.role_for_slot(slot_index)?,
                    Bytes::from(bytes),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let original_size = u64::try_from(input.len()).map_err(|_| CodecError::InvalidInput {
            reason: "input size does not fit in u64".to_string(),
        })?;
        let shard_size = shards.first().map_or(0, |shard| shard.size);
        Ok(EncodedChunk {
            chunk_index,
            original_size,
            shard_size,
            shards,
        })
    }

    fn decode_chunk(
        &self,
        shards: &[Option<Shard>],
        original_size: u64,
    ) -> Result<Bytes, CodecError> {
        let candidates = ValidShardSet::new(&self.profile, shards)?;
        let decoded = self.decode_data_shards(candidates.present())?;
        truncate_data_shards(decoded, original_size)
    }

    fn repair_shard(
        &self,
        shards: &[Option<Shard>],
        missing_slot: u8,
    ) -> Result<Shard, CodecError> {
        ensure_slot_in_profile(&self.profile, missing_slot)?;
        let candidates = ValidShardSet::new(&self.profile, shards)?;
        ensure_slot_is_absent(&candidates, missing_slot)?;
        let data_shards = self.decode_data_shards(candidates.present())?;
        let repaired = self.encode_slot(missing_slot, &data_shards)?;
        Shard::new(
            missing_slot,
            candidates.chunk_index(),
            self.profile.role_for_slot(missing_slot)?,
            Bytes::from(repaired),
        )
    }

    fn failure_matrix(&self) -> FailureMatrix {
        reed_solomon_failure_matrix(&self.profile)
    }

    fn recovery_path(&self, lost: &[bool]) -> RecoveryPath {
        reed_solomon_recovery_path(&self.profile, lost)
    }
}

/// Applies `rows` coefficient rows to `sources`, one output buffer per row.
///
/// `coefficients` is the row-major matrix: `rows * sources.len()` bytes. Tables
/// are built per call, which is why the encode path keeps its own instead.
fn apply_rows(
    coefficients: &[u8],
    rows: usize,
    sources: &[&[u8]],
) -> Result<Vec<Vec<u8>>, CodecError> {
    let shard_size = sources.first().map_or(0, |source| source.len());
    if shard_size == 0 {
        // ISA-L rejects a zero length, while the portable engine returns empty
        // buffers. Only a hand-built shard set can get here, but the two
        // engines still have to agree, so answer the way it does.
        return Ok(vec![Vec::new(); rows]);
    }

    let tables = ffi::init_tables(sources.len(), rows, coefficients)?;
    let mut output = vec![vec![0u8; shard_size]; rows];
    ffi::encode_data(&tables, sources, &mut output)?;
    Ok(output)
}

/// Flattens matrix rows into the row-major buffer ISA-L expects.
fn flatten_rows(rows: &[Vec<u8>]) -> Vec<u8> {
    rows.iter().flatten().copied().collect()
}

/// Borrows owned shards as the slices the FFI wrappers take.
fn as_slices(shards: &[Vec<u8>]) -> Vec<&[u8]> {
    shards.iter().map(Vec::as_slice).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload() -> Vec<u8> {
        (0..10_000).map(|idx| (idx % 251) as u8).collect()
    }

    fn options_without(encoded: &EncodedChunk, missing: &[u8]) -> Vec<Option<Shard>> {
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

    #[test]
    fn lrc_profiles_are_rejected() {
        let error = IsalReedSolomonCodec::new(EcProfileSnapshot::lrc_standard()).unwrap_err();
        assert!(error.to_string().contains("requires a Reed-Solomon profile"));
    }

    /// `encode_chunk` stores data shards untouched, so a matrix whose top block
    /// is not the identity would produce shards its own decode cannot invert.
    #[test]
    fn non_systematic_matrices_are_rejected() {
        let profile = EcProfileSnapshot::rs_small();
        let mut generator = systematic_generator_matrix(profile.k, profile.total_shards()).unwrap();
        generator[0][1] ^= 1;

        let error = IsalReedSolomonCodec::with_generator_matrix(profile, generator).unwrap_err();
        assert!(error.to_string().contains("identity"), "{error}");
    }

    #[test]
    fn matrices_of_the_wrong_shape_are_rejected() {
        let profile = EcProfileSnapshot::rs_small();
        let generator = vec![vec![1u8; usize::from(profile.k)]; 2];
        let error = IsalReedSolomonCodec::with_generator_matrix(profile, generator).unwrap_err();
        assert!(error.to_string().contains("row(s)"), "{error}");
    }

    /// A different but still systematic matrix is what the EC test hook feeds
    /// in: data shards must survive it, parity must not.
    #[test]
    fn a_foreign_systematic_matrix_changes_parity_only() {
        let profile = EcProfileSnapshot::rs_small();
        let foreign =
            s4_codec::test_hooks::foreign_generator_matrix(profile.k, profile.total_shards())
                .unwrap();

        let stored = IsalReedSolomonCodec::new(profile.clone()).unwrap();
        let wrong = IsalReedSolomonCodec::with_generator_matrix(profile.clone(), foreign).unwrap();

        let expected = stored.encode_chunk(&payload(), 0).unwrap();
        let actual = wrong.encode_chunk(&payload(), 0).unwrap();

        for slot in 0..profile.k {
            assert_eq!(
                expected.shards[usize::from(slot)].data,
                actual.shards[usize::from(slot)].data,
                "data slot {slot} must be untouched"
            );
        }
        for slot in profile.k..profile.total_shards() {
            assert_ne!(
                expected.shards[usize::from(slot)].data,
                actual.shards[usize::from(slot)].data,
                "parity slot {slot} must differ, or the hook proves nothing"
            );
        }
    }

    #[test]
    fn tables_cover_every_parity_row() {
        let profile = EcProfileSnapshot::rs_standard();
        let codec = IsalReedSolomonCodec::new(profile.clone()).unwrap();
        let parity_rows = usize::from(profile.total_shards() - profile.k);
        assert_eq!(codec.generator.len(), usize::from(profile.total_shards()));
        assert_eq!(
            codec.parity_tables.len(),
            32 * usize::from(profile.k) * parity_rows
        );
    }

    #[test]
    fn data_shards_pass_through_unchanged() {
        // The top of the generator matrix is the identity, so a data slot must
        // come back exactly as it went in.
        let profile = EcProfileSnapshot::rs_small();
        let codec = IsalReedSolomonCodec::new(profile.clone()).unwrap();
        let input = payload();
        let encoded = codec.encode_chunk(&input, 0).unwrap();
        let split = split_data_shards(&input, profile.k).unwrap();

        for (slot, shard) in split.iter().enumerate() {
            assert_eq!(encoded.shards[slot].data.as_ref(), shard.as_slice());
        }
    }

    #[test]
    fn presets_decode_after_losing_all_parity_shards() {
        for profile in [
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
        ] {
            let codec = IsalReedSolomonCodec::new(profile.clone()).unwrap();
            let encoded = codec.encode_chunk(&payload(), 7).unwrap();
            let missing = (profile.k..profile.total_shards()).collect::<Vec<_>>();
            let decoded = codec
                .decode_chunk(&options_without(&encoded, &missing), encoded.original_size)
                .unwrap();
            assert_eq!(decoded.as_ref(), payload().as_slice());
        }
    }

    #[test]
    fn decodes_from_any_k_valid_shards() {
        let codec = IsalReedSolomonCodec::new(EcProfileSnapshot::rs_standard()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 1).unwrap();
        let decoded = codec
            .decode_chunk(
                &options_without(&encoded, &[0, 3, 8]),
                encoded.original_size,
            )
            .unwrap();
        assert_eq!(decoded.as_ref(), payload().as_slice());
    }

    #[test]
    fn rejects_fewer_than_k_shards() {
        let codec = IsalReedSolomonCodec::new(EcProfileSnapshot::rs_small()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 1).unwrap();
        let error = codec
            .decode_chunk(
                &options_without(&encoded, &[0, 1, 2]),
                encoded.original_size,
            )
            .unwrap_err();
        assert!(error.to_string().contains("need at least"));
    }

    #[test]
    fn repairs_data_and_parity_shards() {
        let codec = IsalReedSolomonCodec::new(EcProfileSnapshot::rs_small()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 9).unwrap();
        for missing_slot in [0, 5] {
            let repaired = codec
                .repair_shard(&options_without(&encoded, &[missing_slot]), missing_slot)
                .unwrap();
            assert_eq!(repaired, encoded.shards[usize::from(missing_slot)]);
        }
    }

    #[test]
    fn repair_rejects_unknown_and_present_slots() {
        let codec = IsalReedSolomonCodec::new(EcProfileSnapshot::rs_small()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 3).unwrap();

        let error = codec.repair_shard(&options_without(&encoded, &[]), 99).unwrap_err();
        assert!(error.to_string().contains("outside profile"));

        let error = codec.repair_shard(&options_without(&encoded, &[]), 2).unwrap_err();
        assert!(error.to_string().contains("is not missing"));
    }

    #[test]
    fn corrupt_shards_are_refused_before_decoding() {
        let codec = IsalReedSolomonCodec::new(EcProfileSnapshot::rs_small()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 3).unwrap();

        let mut shards = options_without(&encoded, &[]);
        shards[1].as_mut().unwrap().crc32 ^= 1;
        let error = codec.decode_chunk(&shards, encoded.original_size).unwrap_err();
        assert!(error.to_string().contains("crc32"));
    }
}
