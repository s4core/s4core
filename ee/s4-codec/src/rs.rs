// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! MDS Reed-Solomon codec backend.

use bytes::Bytes;

use crate::gf;
use crate::types::{
    CodecEngine, CodecError, EcCodecKind, EcProfileSnapshot, EncodedChunk, FailureMatrix,
    FailureMatrixEntry, RecoveryPath, Shard,
};

/// Reed-Solomon codec engine using a systematic Vandermonde generator matrix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReedSolomonCodec {
    profile: EcProfileSnapshot,
    math: ReedSolomonMath,
}

impl ReedSolomonCodec {
    /// Build a Reed-Solomon codec engine.
    pub fn new(profile: EcProfileSnapshot) -> Result<Self, CodecError> {
        profile.validate()?;
        if profile.codec != EcCodecKind::ReedSolomon {
            return Err(CodecError::InvalidProfile {
                profile: profile.name.clone(),
                reason: "ReedSolomonCodec requires a Reed-Solomon profile".to_string(),
            });
        }
        let total_shards = profile.total_shards();
        let math = ReedSolomonMath::new(profile.k, total_shards)?;
        Ok(Self { profile, math })
    }
}

impl CodecEngine for ReedSolomonCodec {
    fn profile(&self) -> &EcProfileSnapshot {
        &self.profile
    }

    fn encode_chunk(&self, input: &[u8], chunk_index: u32) -> Result<EncodedChunk, CodecError> {
        let data = split_data_shards(input, self.profile.k)?;
        let encoded = self.math.encode_data_shards(&data)?;
        let shards = encoded
            .into_iter()
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
        let decoded = self.math.decode_data_shards(&candidates.present)?;
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
        let data_shards = self.math.decode_data_shards(&candidates.present)?;
        let repaired = self.math.encode_slot(missing_slot, &data_shards)?;
        Shard::new(
            missing_slot,
            candidates.chunk_index,
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

impl ReedSolomonCodec {
    /// Whether `decode_chunk` rebuilds a chunk once the slots marked in `lost`
    /// are gone: any `k` of the `k + parity` shards are enough, so no more
    /// than `parity` may be missing.
    pub(crate) fn decodes_after_loss(parity: u8, lost: &[bool]) -> bool {
        lost.iter().filter(|lost| **lost).count() <= usize::from(parity)
    }
}

/// Failure matrix documented by every Reed-Solomon backend.
///
/// Lives outside the engine so that an accelerated backend reports exactly the
/// same patterns, in the same order, instead of restating them and drifting.
pub fn reed_solomon_failure_matrix(profile: &EcProfileSnapshot) -> FailureMatrix {
    let total = profile.total_shards();
    let parity = total - profile.k;
    let guaranteed_missing = (profile.k..total).collect::<Vec<_>>();
    let non_recoverable = (0..=parity).collect::<Vec<_>>();

    // Both entries take their verdict and their path from the slots, so the
    // document cannot claim a recovery the decoder would refuse.
    let entry = |label: &str, missing_slots: Vec<u8>, reason: String| {
        let recovery_path = reed_solomon_recovery_path(profile, &lost_flags(total, &missing_slots));
        FailureMatrixEntry {
            label: label.to_string(),
            missing_slots,
            recoverable: recovery_path.recovers(),
            recovery_path,
            reason,
        }
    };

    FailureMatrix::new(
        profile,
        vec![
            entry(
                "any-k-of-n",
                guaranteed_missing,
                format!(
                    "RS can reconstruct from any {} of {total} shards",
                    profile.k
                ),
            ),
            entry(
                "fewer-than-k",
                non_recoverable,
                format!("losing more than {parity} shard(s) leaves fewer than k data equations"),
            ),
        ],
    )
}

/// How a Reed-Solomon profile recovers one loss pattern.
///
/// The code is MDS, so only two things matter: how many shards are gone, and
/// whether any of them held data. Losing parity alone leaves every data shard
/// in place and costs nothing to read.
///
/// Lives outside the engines, like [`reed_solomon_failure_matrix`] and for the
/// same reason: an accelerated node and a portable one must not be able to
/// describe the same loss differently.
pub fn reed_solomon_recovery_path(profile: &EcProfileSnapshot, lost: &[bool]) -> RecoveryPath {
    let is_lost = |slot: u8| lost.get(usize::from(slot)).copied().unwrap_or(false);
    if !ReedSolomonCodec::decodes_after_loss(profile.total_shards() - profile.k, lost) {
        RecoveryPath::None
    } else if (0..profile.k).all(|slot| !is_lost(slot)) {
        RecoveryPath::Direct
    } else {
        RecoveryPath::GlobalRs
    }
}

/// The loss pattern `missing_slots` as the flags every predicate reads.
///
/// One shape for a loss pattern rather than two: a list of slots can hold the
/// same slot twice or a slot the profile does not have, and a counting rule
/// reading it would answer for a pattern that cannot happen.
pub(crate) fn lost_flags(total_shards: u8, missing_slots: &[u8]) -> Vec<bool> {
    (0..total_shards).map(|slot| missing_slots.contains(&slot)).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReedSolomonMath {
    k: u8,
    total_shards: u8,
    generator: Vec<Vec<u8>>,
}

impl ReedSolomonMath {
    pub(crate) fn new(k: u8, total_shards: u8) -> Result<Self, CodecError> {
        if k == 0 || total_shards <= k {
            return Err(CodecError::InvalidProfile {
                profile: "rs-math".to_string(),
                reason: "RS math requires total_shards > k > 0".to_string(),
            });
        }
        let generator = systematic_generator_matrix(k, total_shards)?;
        Ok(Self {
            k,
            total_shards,
            generator,
        })
    }

    pub(crate) fn encode_data_shards(
        &self,
        data_shards: &[Vec<u8>],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        self.validate_data_shards(data_shards)?;
        (0..self.total_shards).map(|slot| self.encode_slot(slot, data_shards)).collect()
    }

    pub(crate) fn encode_slot(
        &self,
        slot: u8,
        data_shards: &[Vec<u8>],
    ) -> Result<Vec<u8>, CodecError> {
        self.validate_data_shards(data_shards)?;
        let row =
            self.generator
                .get(usize::from(slot))
                .ok_or_else(|| CodecError::InvalidShardSet {
                    reason: format!("slot {slot} is outside RS generator matrix"),
                })?;
        Ok(combine(row, data_shards))
    }

    pub(crate) fn decode_data_shards(
        &self,
        candidates: &[ShardCandidate],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        ensure_enough_candidates(self.k, candidates)?;

        let selected = candidates.iter().take(usize::from(self.k)).cloned().collect::<Vec<_>>();
        let matrix = selected
            .iter()
            .map(|candidate| self.generator[usize::from(candidate.slot_index)].clone())
            .collect::<Vec<_>>();
        let inverse = gf::invert_matrix(&matrix)?;
        let encoded = selected.iter().map(|candidate| candidate.data.to_vec()).collect::<Vec<_>>();
        Ok(inverse.iter().map(|row| combine(row, &encoded)).collect::<Vec<_>>())
    }

    fn validate_data_shards(&self, data_shards: &[Vec<u8>]) -> Result<(), CodecError> {
        validate_data_shards(self.k, data_shards).map(|_| ())
    }
}

/// Checks data shards against `k` and returns the size they all share.
///
/// # Errors
///
/// Returns [`CodecError::InvalidInput`] when the count does not match `k`, when
/// there are no shards, or when their sizes differ.
pub fn validate_data_shards(k: u8, data_shards: &[Vec<u8>]) -> Result<usize, CodecError> {
    if data_shards.len() != usize::from(k) {
        return Err(CodecError::InvalidInput {
            reason: format!("expected {k} data shard(s), got {}", data_shards.len()),
        });
    }
    let Some(first) = data_shards.first() else {
        return Err(CodecError::InvalidInput {
            reason: "missing data shards".to_string(),
        });
    };
    if data_shards.iter().any(|shard| shard.len() != first.len()) {
        return Err(CodecError::InvalidInput {
            reason: "all data shards must have the same size".to_string(),
        });
    }
    Ok(first.len())
}

/// Checks that enough shards survived to solve for the data.
///
/// # Errors
///
/// Returns [`CodecError::NotRecoverable`] when fewer than `k` shards are left.
pub fn ensure_enough_candidates(k: u8, candidates: &[ShardCandidate]) -> Result<(), CodecError> {
    if candidates.len() < usize::from(k) {
        return Err(CodecError::NotRecoverable {
            reason: format!("need at least {k} valid shard(s), got {}", candidates.len()),
        });
    }
    Ok(())
}

/// Checks that a repair request names a slot the profile actually has.
///
/// # Errors
///
/// Returns [`CodecError::InvalidShardSet`] when the slot is outside the profile.
pub fn ensure_slot_in_profile(
    profile: &EcProfileSnapshot,
    missing_slot: u8,
) -> Result<(), CodecError> {
    if missing_slot >= profile.total_shards() {
        return Err(CodecError::InvalidShardSet {
            reason: format!("missing slot {missing_slot} is outside profile"),
        });
    }
    Ok(())
}

/// Checks that the slot being repaired is in fact missing from the set.
///
/// # Errors
///
/// Returns [`CodecError::InvalidShardSet`] when the slot is present, because
/// repairing a shard that is already there would silently overwrite it.
pub fn ensure_slot_is_absent(shards: &ValidShardSet, missing_slot: u8) -> Result<(), CodecError> {
    if shards.present.iter().any(|candidate| candidate.slot_index == missing_slot) {
        return Err(CodecError::InvalidShardSet {
            reason: format!("slot {missing_slot} is not missing"),
        });
    }
    Ok(())
}

/// One shard that passed validation, reduced to what matrix decoders need.
///
/// The fields are read-only on purpose: a candidate exists only because
/// [`ValidShardSet::new`] checked the shard it came from.
#[derive(Debug, Clone)]
pub struct ShardCandidate {
    pub(crate) slot_index: u8,
    pub(crate) data: Bytes,
}

impl ShardCandidate {
    /// Slot this shard occupies in the erasure set.
    #[must_use]
    pub fn slot_index(&self) -> u8 {
        self.slot_index
    }

    /// Shard bytes.
    #[must_use]
    pub fn data(&self) -> &Bytes {
        &self.data
    }
}

/// Shards of one chunk that are present and have passed every check.
///
/// Building this set is the shared entry point for decode and repair: it
/// rejects mismatched slots, wrong roles, mixed chunk indexes, mixed shard
/// sizes and broken checksums, so a backend never has to repeat those rules.
///
/// There is no way to assemble one without those checks — the fields are
/// crate-private and readable through accessors. An accelerated backend that
/// could build the set by hand would be able to decode a shard whose checksum
/// no longer matches its bytes, and the object rebuilt from it would carry
/// freshly computed hashes that hide the damage.
pub struct ValidShardSet {
    pub(crate) chunk_index: u32,
    pub(crate) present: Vec<ShardCandidate>,
}

impl ValidShardSet {
    /// Chunk index every present shard agrees on.
    #[must_use]
    pub fn chunk_index(&self) -> u32 {
        self.chunk_index
    }

    /// Present shards, sorted by slot index.
    #[must_use]
    pub fn present(&self) -> &[ShardCandidate] {
        &self.present
    }

    /// Validate the shard slots of one chunk against a profile.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::InvalidShardSet`] when the slots do not match the
    /// profile or disagree with each other, [`CodecError::HashMismatch`] when a
    /// shard fails its own checksum, and [`CodecError::NotRecoverable`] when no
    /// shard is present at all.
    pub fn new(profile: &EcProfileSnapshot, shards: &[Option<Shard>]) -> Result<Self, CodecError> {
        if shards.len() != usize::from(profile.total_shards()) {
            return Err(CodecError::InvalidShardSet {
                reason: format!(
                    "expected {} shard slot(s), got {}",
                    profile.total_shards(),
                    shards.len()
                ),
            });
        }

        let mut chunk_index = None;
        let mut shard_size = None;
        let mut present = Vec::new();
        for (slot, shard) in shards.iter().enumerate() {
            let Some(shard) = shard else {
                continue;
            };
            let expected_slot = u8::try_from(slot).map_err(|_| CodecError::InvalidShardSet {
                reason: "slot index does not fit in u8".to_string(),
            })?;
            if shard.slot_index != expected_slot {
                return Err(CodecError::InvalidShardSet {
                    reason: format!("shard for slot {slot} declares slot {}", shard.slot_index),
                });
            }
            let expected_role = profile.role_for_slot(shard.slot_index)?;
            if shard.role != expected_role {
                return Err(CodecError::InvalidShardSet {
                    reason: format!(
                        "slot {} has role {:?}, expected {:?}",
                        shard.slot_index, shard.role, expected_role
                    ),
                });
            }
            shard.verify_integrity()?;

            match chunk_index {
                Some(expected) if expected != shard.chunk_index => {
                    return Err(CodecError::InvalidShardSet {
                        reason: format!(
                            "mixed chunk indexes: {expected} and {}",
                            shard.chunk_index
                        ),
                    });
                }
                None => chunk_index = Some(shard.chunk_index),
                Some(_) => {}
            }
            match shard_size {
                Some(expected) if expected != shard.size => {
                    return Err(CodecError::InvalidShardSet {
                        reason: format!("mixed shard sizes: {expected} and {}", shard.size),
                    });
                }
                None => shard_size = Some(shard.size),
                Some(_) => {}
            }
            present.push(ShardCandidate {
                slot_index: shard.slot_index,
                data: shard.data.clone(),
            });
        }

        let chunk_index = chunk_index.ok_or_else(|| CodecError::NotRecoverable {
            reason: "no shards are present".to_string(),
        })?;
        present.sort_by_key(|candidate| candidate.slot_index);
        Ok(Self {
            chunk_index,
            present,
        })
    }
}

/// Cuts a chunk into `k` equally sized data shards.
///
/// Shards are `ceil(len / k)` bytes, never zero, filled in order: shard `i`
/// takes the bytes at offset `i * shard_size`. The tail of the last shard is
/// padded with zeros, so an empty input still yields `k` single-zero-byte
/// shards. [`truncate_data_shards`] undoes the padding on the way back.
///
/// # Errors
///
/// Returns [`CodecError::InvalidInput`] when `k` is zero.
pub fn split_data_shards(input: &[u8], k: u8) -> Result<Vec<Vec<u8>>, CodecError> {
    if k == 0 {
        return Err(CodecError::InvalidInput {
            reason: "k must be greater than 0".to_string(),
        });
    }
    let k_usize = usize::from(k);
    let shard_size = input.len().div_ceil(k_usize).max(1);
    let mut shards = vec![vec![0; shard_size]; k_usize];
    for (idx, byte) in input.iter().enumerate() {
        let shard = idx / shard_size;
        let offset = idx % shard_size;
        shards[shard][offset] = *byte;
    }
    Ok(shards)
}

/// Joins data shards back together and drops the padding added when splitting.
///
/// # Errors
///
/// Returns [`CodecError::InvalidInput`] when `original_size` does not fit in a
/// `usize` or exceeds the bytes the shards actually hold.
pub fn truncate_data_shards(
    data_shards: Vec<Vec<u8>>,
    original_size: u64,
) -> Result<Bytes, CodecError> {
    let original_size = usize::try_from(original_size).map_err(|_| CodecError::InvalidInput {
        reason: "original size does not fit in usize".to_string(),
    })?;
    let total_len = data_shards.iter().map(Vec::len).sum::<usize>();
    if original_size > total_len {
        return Err(CodecError::InvalidInput {
            reason: format!("original size {original_size} exceeds decoded capacity {total_len}"),
        });
    }
    let mut decoded = Vec::with_capacity(original_size);
    for shard in data_shards {
        let remaining = original_size.saturating_sub(decoded.len());
        if remaining == 0 {
            break;
        }
        decoded.extend_from_slice(&shard[..remaining.min(shard.len())]);
    }
    Ok(Bytes::from(decoded))
}

pub(crate) fn combine(coefficients: &[u8], shards: &[Vec<u8>]) -> Vec<u8> {
    let shard_size = shards.first().map_or(0, Vec::len);
    let mut output = vec![0; shard_size];
    for (coefficient, shard) in coefficients.iter().zip(shards) {
        if *coefficient == 0 {
            continue;
        }
        if *coefficient == 1 {
            for (idx, byte) in shard.iter().enumerate() {
                output[idx] ^= *byte;
            }
            continue;
        }
        for (idx, byte) in shard.iter().enumerate() {
            output[idx] ^= gf::mul(*coefficient, *byte);
        }
    }
    output
}

/// Builds the systematic generator matrix that defines the stored shard bytes.
///
/// The matrix has `total_shards` rows of `k` coefficients. It starts as a
/// Vandermonde matrix over the points `1, 2, 3, …` and is multiplied by the
/// inverse of its own top `k × k` block, which turns that block into the
/// identity. Data shards therefore pass through unchanged and rows `k..n` are
/// the parity coefficients.
///
/// This is the single definition of the on-disk format: every backend, portable
/// or accelerated, must encode with this matrix and no other. See decision
/// EC2-002.
///
/// # Errors
///
/// Returns [`CodecError::Arithmetic`] when the dimensions are unusable, when
/// they cannot form a Vandermonde matrix over GF(2^8), or when the top block is
/// not invertible.
pub fn systematic_generator_matrix(k: u8, total_shards: u8) -> Result<Vec<Vec<u8>>, CodecError> {
    systematic_matrix_from_point(k, total_shards, FIRST_VANDERMONDE_POINT)
}

/// First evaluation point of the Vandermonde matrix S4 encodes with.
///
/// Part of the storage format: changing it changes every parity byte.
pub(crate) const FIRST_VANDERMONDE_POINT: u8 = 1;

/// Builds a systematic generator matrix from Vandermonde points `x, x+1, …`.
///
/// Only the starting point varies. Any starting point yields a valid RS code,
/// which is precisely why the one S4 uses has to be pinned down: a different
/// point produces a matrix that is equally correct and completely incompatible.
/// The parameter exists so the EC test hooks can build such a matrix on purpose
/// — see [`crate::test_hooks`].
pub(crate) fn systematic_matrix_from_point(
    k: u8,
    total_shards: u8,
    first_point: u8,
) -> Result<Vec<Vec<u8>>, CodecError> {
    // Dimensions are checked here rather than in the callers: this function is
    // public, so a profile that came from a manifest or an operator must get an
    // error, not a panic when the top block is sliced off.
    if k == 0 || total_shards < k {
        return Err(CodecError::Arithmetic {
            reason: format!(
                "generator matrix needs 0 < k <= total_shards, got k={k} and {total_shards} shard(s)"
            ),
        });
    }
    let k_usize = usize::from(k);
    let total_usize = usize::from(total_shards);
    let base = (0..total_usize)
        .map(|row| {
            let x = u8::try_from(row)
                .ok()
                .and_then(|row| row.checked_add(first_point))
                .ok_or_else(|| CodecError::Arithmetic {
                    reason: "Vandermonde evaluation point does not fit in u8".to_string(),
                })?;
            Ok((0..k_usize).map(|col| gf::pow(x, col)).collect::<Vec<_>>())
        })
        .collect::<Result<Vec<_>, CodecError>>()?;
    let top = base[..k_usize].to_vec();
    let top_inverse = gf::invert_matrix(&top)?;
    gf::matrix_multiply(&base, &top_inverse)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn payload() -> Vec<u8> {
        (0..10_000).map(|idx| (idx % 251) as u8).collect()
    }

    #[test]
    fn rs_presets_decode_after_losing_all_parity_shards() {
        for profile in [
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
        ] {
            let codec = ReedSolomonCodec::new(profile.clone()).unwrap();
            let encoded = codec.encode_chunk(&payload(), 7).unwrap();
            let missing = (profile.k..profile.total_shards()).collect::<Vec<_>>();
            let decoded = codec
                .decode_chunk(&options_without(&encoded, &missing), encoded.original_size)
                .unwrap();
            assert_eq!(decoded.as_ref(), payload().as_slice());
        }
    }

    #[test]
    fn rs_decodes_from_any_k_valid_shards() {
        let profile = EcProfileSnapshot::rs_standard();
        let codec = ReedSolomonCodec::new(profile.clone()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 1).unwrap();
        let missing = vec![0, 3, 8];
        let decoded = codec
            .decode_chunk(&options_without(&encoded, &missing), encoded.original_size)
            .unwrap();
        assert_eq!(decoded.as_ref(), payload().as_slice());
    }

    #[test]
    fn rs_rejects_fewer_than_k_shards() {
        let profile = EcProfileSnapshot::rs_small();
        let codec = ReedSolomonCodec::new(profile.clone()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 1).unwrap();
        let missing = vec![0, 1, 2];
        let err = codec
            .decode_chunk(&options_without(&encoded, &missing), encoded.original_size)
            .unwrap_err();
        assert!(err.to_string().contains("need at least"));
    }

    #[test]
    fn rs_repairs_data_and_parity_shards() {
        let profile = EcProfileSnapshot::rs_small();
        let codec = ReedSolomonCodec::new(profile).unwrap();
        let encoded = codec.encode_chunk(&payload(), 9).unwrap();
        for missing_slot in [0, 5] {
            let repaired = codec
                .repair_shard(&options_without(&encoded, &[missing_slot]), missing_slot)
                .unwrap();
            assert_eq!(repaired, encoded.shards[usize::from(missing_slot)]);
        }
    }

    #[test]
    fn rs_rejects_invalid_slot_and_hashes() {
        let codec = ReedSolomonCodec::new(EcProfileSnapshot::rs_small()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 3).unwrap();
        let mut shards = options_without(&encoded, &[]);
        shards[1].as_mut().unwrap().slot_index = 2;
        let err = codec.decode_chunk(&shards, encoded.original_size).unwrap_err();
        assert!(err.to_string().contains("declares slot"));

        let mut shards = options_without(&encoded, &[]);
        shards[1].as_mut().unwrap().crc32 ^= 1;
        let err = codec.decode_chunk(&shards, encoded.original_size).unwrap_err();
        assert!(err.to_string().contains("crc32"));
    }
}
