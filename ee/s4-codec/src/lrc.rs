// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Locally repairable codec backend.

use bytes::Bytes;

use crate::lrc_solver::LrcSolver;
use crate::rs::{
    ensure_slot_in_profile, ensure_slot_is_absent, lost_flags, split_data_shards,
    truncate_data_shards, ReedSolomonMath, ShardCandidate, ValidShardSet,
};
use crate::types::{
    CodecEngine, CodecError, EcCodecKind, EcParity, EcProfileSnapshot, EncodedChunk, FailureMatrix,
    FailureMatrixEntry, RecoveryPath, Shard, ShardRole,
};

/// How far a codec's recovery reaches.
///
/// A node either runs the unified solver behind its fast paths or stops where
/// EC v1 stopped. The second is not a production choice: only the
/// mixed-cluster test hook selects it, so that one build can stand in for both
/// sides of a rolling upgrade. The choice is made once, when the codec is
/// built, because a node does not change edition while it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryReach {
    /// The fast paths, and the unified solver behind them.
    Unified,
    /// The two fast paths alone — everything an EC v1 node reached.
    FastPathsOnly,
}

impl RecoveryReach {
    /// What this process reaches: everything, unless the mixed-cluster hook
    /// says otherwise.
    pub(crate) fn of_this_process() -> Self {
        if crate::test_hooks::lrc_fast_path_only() {
            Self::FastPathsOnly
        } else {
            Self::Unified
        }
    }

    /// Whether the unified solver may run at all.
    fn solves(self) -> bool {
        self == Self::Unified
    }
}

/// LRC codec engine with XOR local parity and RS global parity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LrcCodec {
    profile: EcProfileSnapshot,
    local_groups: u8,
    global_parity: u8,
    data_per_group: u8,
    global_math: ReedSolomonMath,
    /// The whole code as one system of equations, used once the fast paths
    /// have run out. Built with the codec, because it depends on the profile
    /// alone and every chunk asks it the same question.
    solver: LrcSolver,
    /// Whether the solver above is allowed to run.
    reach: RecoveryReach,
}

impl LrcCodec {
    /// Build an LRC codec engine.
    pub fn new(profile: EcProfileSnapshot) -> Result<Self, CodecError> {
        Self::with_reach(profile, RecoveryReach::of_this_process())
    }

    /// Build an LRC codec engine that recovers only as far as `reach`.
    ///
    /// Encoding does not depend on `reach`: a codec held to the fast paths
    /// writes the same shards as any other, which is why the mixed-cluster
    /// hook is safe to arm on a node that also takes writes.
    pub(crate) fn with_reach(
        profile: EcProfileSnapshot,
        reach: RecoveryReach,
    ) -> Result<Self, CodecError> {
        profile.validate()?;
        if profile.codec != EcCodecKind::LocallyRepairable {
            return Err(CodecError::InvalidProfile {
                profile: profile.name.clone(),
                reason: "LrcCodec requires an LRC profile".to_string(),
            });
        }
        let EcParity::Lrc { local, global } = profile.parity else {
            return Err(CodecError::InvalidProfile {
                profile: profile.name.clone(),
                reason: "LrcCodec requires LRC parity".to_string(),
            });
        };
        let data_per_group = profile.k / local;
        let global_math = ReedSolomonMath::new(profile.k, profile.k + global)?;
        let solver = LrcSolver::new(profile.k, local, global)?;
        Ok(Self {
            profile,
            local_groups: local,
            global_parity: global,
            data_per_group,
            global_math,
            solver,
            reach,
        })
    }

    fn local_parity_slot(&self, group: u8) -> u8 {
        self.profile.k + group
    }

    fn global_parity_slot(&self, global_index: u8) -> u8 {
        self.profile.k + self.local_groups + global_index
    }

    fn data_slots_for_group(&self, group: u8) -> std::ops::Range<u8> {
        let start = group * self.data_per_group;
        start..(start + self.data_per_group)
    }

    fn local_parity(&self, data_shards: &[Vec<u8>], group: u8) -> Vec<u8> {
        let shard_size = data_shards.first().map_or(0, Vec::len);
        let mut parity = vec![0; shard_size];
        for slot in self.data_slots_for_group(group) {
            for (idx, byte) in data_shards[usize::from(slot)].iter().enumerate() {
                parity[idx] ^= *byte;
            }
        }
        parity
    }

    fn reconstruct_data(&self, valid: &ValidShardSet) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut data = vec![None; usize::from(self.profile.k)];
        let mut local = vec![None; usize::from(self.local_groups)];
        let mut global_candidates = Vec::new();

        for candidate in &valid.present {
            if candidate.slot_index < self.profile.k {
                data[usize::from(candidate.slot_index)] = Some(candidate.data.to_vec());
                global_candidates.push(ShardCandidate {
                    slot_index: candidate.slot_index,
                    data: candidate.data.clone(),
                });
            } else if candidate.slot_index < self.profile.k + self.local_groups {
                let group = candidate.slot_index - self.profile.k;
                local[usize::from(group)] = Some(candidate.data.to_vec());
            } else {
                let global_index = candidate.slot_index - self.profile.k - self.local_groups;
                global_candidates.push(ShardCandidate {
                    slot_index: self.profile.k + global_index,
                    data: candidate.data.clone(),
                });
            }
        }

        if data.iter().all(Option::is_some) {
            return data
                .into_iter()
                .map(|shard| {
                    shard.ok_or_else(|| CodecError::Arithmetic {
                        reason: "data shard disappeared during LRC reconstruction".to_string(),
                    })
                })
                .collect();
        }

        self.repair_with_local_parity(&mut data, &local)?;
        if data.iter().all(Option::is_some) {
            return data
                .into_iter()
                .map(|shard| {
                    shard.ok_or_else(|| CodecError::Arithmetic {
                        reason: "data shard disappeared after local repair".to_string(),
                    })
                })
                .collect();
        }

        global_candidates.retain(|candidate| candidate.slot_index >= self.profile.k);
        for (slot, shard) in data.iter().enumerate() {
            if let Some(shard) = shard {
                let slot_index = u8::try_from(slot).map_err(|_| CodecError::Arithmetic {
                    reason: "LRC data slot index does not fit in u8".to_string(),
                })?;
                global_candidates.push(ShardCandidate {
                    slot_index,
                    data: Bytes::from(shard.clone()),
                });
            }
        }
        global_candidates.sort_by_key(|candidate| candidate.slot_index);
        if global_candidates.len() >= usize::from(self.profile.k) {
            return self.global_math.decode_data_shards(&global_candidates);
        }

        // Both fast stages have run out, and each of them left an equation
        // unused: the local stage skips a group missing more than one data
        // shard, and the global stage never looks at local parity at all. The
        // unified solver puts every surviving row into one system and answers
        // by its rank, so it reaches everything the fast paths reach and the
        // patterns that fall between them (decision EC2-101). Short of rank it
        // refuses rather than completing the data with a guess.
        self.ensure_solver_may_run()?;
        self.solver.recover_data_shards(&valid.present)
    }

    fn repair_with_local_parity(
        &self,
        data: &mut [Option<Vec<u8>>],
        local: &[Option<Vec<u8>>],
    ) -> Result<(), CodecError> {
        // `fast_path_after_loss` below repeats this loop without shards;
        // change them together.
        let mut changed = true;
        while changed {
            changed = false;
            for group in 0..self.local_groups {
                let missing = self
                    .data_slots_for_group(group)
                    .filter(|slot| data[usize::from(*slot)].is_none())
                    .collect::<Vec<_>>();
                if missing.len() != 1 {
                    continue;
                }
                let Some(local_parity) = &local[usize::from(group)] else {
                    continue;
                };
                let mut repaired = local_parity.clone();
                for slot in self.data_slots_for_group(group) {
                    if slot == missing[0] {
                        continue;
                    }
                    let Some(shard) = &data[usize::from(slot)] else {
                        continue;
                    };
                    if shard.len() != repaired.len() {
                        return Err(CodecError::InvalidShardSet {
                            reason: "mixed shard sizes during local repair".to_string(),
                        });
                    }
                    for (idx, byte) in shard.iter().enumerate() {
                        repaired[idx] ^= *byte;
                    }
                }
                data[usize::from(missing[0])] = Some(repaired);
                changed = true;
            }
        }
        Ok(())
    }

    /// Which fast path rebuilds every data shard once the slots marked in
    /// `lost` are gone, or [`RecoveryPath::None`] when neither reaches them and
    /// [`Self::reconstruct_data`] has to run the unified solver.
    ///
    /// Answers without shards, by the same two stages the decoder tries first:
    /// XOR inside every local group that misses exactly one data shard and
    /// still has its local parity, repeated until nothing changes; then global
    /// Reed-Solomon, which needs no more missing data shards than live global
    /// parity shards. Local parity takes no part in the second stage — that is
    /// exactly the equation the unified solver picks up.
    ///
    /// Kept next to the decoder so the two change together. This is what tells
    /// an operator the cost of a degraded read, and what
    /// [`CodecEngine::failure_matrix`] names each documented pattern by. The
    /// question of whether the chunk decodes at all belongs to
    /// [`crate::LossTolerance::can_decode`], which asks the solver.
    pub(crate) fn fast_path_after_loss(
        k: u8,
        local: u8,
        global: u8,
        lost: &[bool],
    ) -> RecoveryPath {
        let (k, local, global) = (usize::from(k), usize::from(local), usize::from(global));
        let is_lost = |slot: usize| lost.get(slot).copied().unwrap_or(false);
        let data_per_group = k / local;
        let all_rebuilt = |missing: &[bool]| !missing.contains(&true);

        let mut data_missing = (0..k).map(is_lost).collect::<Vec<_>>();
        if all_rebuilt(&data_missing) {
            return RecoveryPath::Direct;
        }

        let mut changed = true;
        while changed {
            changed = false;
            for group in 0..local {
                if is_lost(k + group) {
                    continue;
                }
                let group_slots = group * data_per_group..(group + 1) * data_per_group;
                let mut missing = group_slots.filter(|slot| data_missing[*slot]);
                if let (Some(only), None) = (missing.next(), missing.next()) {
                    data_missing[only] = false;
                    changed = true;
                }
            }
        }
        if all_rebuilt(&data_missing) {
            return RecoveryPath::LocalXor;
        }

        let still_missing = data_missing.iter().filter(|missing| **missing).count();
        let live_global = (0..global).filter(|index| !is_lost(k + local + index)).count();
        if still_missing <= live_global {
            RecoveryPath::GlobalRs
        } else {
            RecoveryPath::None
        }
    }

    /// Refuses before a solve when this node recovers like an EC v1 one.
    ///
    /// Reached only after both fast paths have run out, so on a node that
    /// reaches everything this costs one comparison per degraded chunk and
    /// nothing at all on a healthy read. The refusal is the same
    /// [`CodecError::NotRecoverable`] the solver itself returns when the
    /// surviving rows fall short: to the caller, a pattern this node cannot
    /// reach and a pattern nobody can reach fail the same way, which is what
    /// makes a half-upgraded cluster safe (decision EC2-103, section 7).
    fn ensure_solver_may_run(&self) -> Result<(), CodecError> {
        if self.reach.solves() {
            return Ok(());
        }
        Err(CodecError::NotRecoverable {
            reason: format!(
                "{}: the EC v1 fast paths do not reach this loss pattern, and the unified solver is disabled on this node",
                self.profile.name
            ),
        })
    }

    /// The slots XOR rebuilds `slot` from inside its local group, or `None`
    /// when `slot` has no local repair — a global parity slot, or a slot past
    /// the end.
    ///
    /// A data slot takes the other data slots of its group and the group's
    /// local parity; a local parity slot takes the data slots of its group.
    /// Either way the missing shard is the XOR of the sources, and there are
    /// `k / local` of them. [`CodecEngine::repair_shard`] takes exactly this
    /// path whenever all of these slots are present; change the two together.
    pub(crate) fn local_repair_sources(k: u8, local: u8, slot: u8) -> Option<Vec<u8>> {
        let data_per_group = k / local;
        let group_data = |group: u8| group * data_per_group..(group + 1) * data_per_group;
        if slot < k {
            let group = slot / data_per_group;
            let mut sources =
                group_data(group).filter(|member| *member != slot).collect::<Vec<_>>();
            sources.push(k + group);
            Some(sources)
        } else if slot < k + local {
            Some(group_data(slot - k).collect())
        } else {
            None
        }
    }

    fn encode_from_data(
        &self,
        data_shards: &[Vec<u8>],
        chunk_index: u32,
    ) -> Result<Vec<Shard>, CodecError> {
        let global_encoded = self.global_math.encode_data_shards(data_shards)?;
        let mut shards = Vec::with_capacity(usize::from(self.profile.total_shards()));
        for slot in 0..self.profile.k {
            shards.push(Shard::new(
                slot,
                chunk_index,
                ShardRole::Data,
                Bytes::from(data_shards[usize::from(slot)].clone()),
            )?);
        }
        for group in 0..self.local_groups {
            shards.push(Shard::new(
                self.local_parity_slot(group),
                chunk_index,
                ShardRole::LrcLocalParity,
                Bytes::from(self.local_parity(data_shards, group)),
            )?);
        }
        for global_index in 0..self.global_parity {
            let virtual_slot = self.profile.k + global_index;
            shards.push(Shard::new(
                self.global_parity_slot(global_index),
                chunk_index,
                ShardRole::LrcGlobalParity,
                Bytes::from(global_encoded[usize::from(virtual_slot)].clone()),
            )?);
        }
        Ok(shards)
    }

    /// One documented pattern, with both its verdict and its path worked out
    /// from the missing slots rather than written down beside them.
    fn matrix_entry(
        &self,
        label: String,
        missing_slots: Vec<u8>,
        reason: String,
    ) -> FailureMatrixEntry {
        let recovery_path =
            self.recovery_path(&lost_flags(self.profile.total_shards(), &missing_slots));
        FailureMatrixEntry {
            label,
            missing_slots,
            recoverable: recovery_path.recovers(),
            recovery_path,
            reason,
        }
    }
}

impl CodecEngine for LrcCodec {
    fn profile(&self) -> &EcProfileSnapshot {
        &self.profile
    }

    fn encode_chunk(&self, input: &[u8], chunk_index: u32) -> Result<EncodedChunk, CodecError> {
        let data = split_data_shards(input, self.profile.k)?;
        let shards = self.encode_from_data(&data, chunk_index)?;
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
        let valid = ValidShardSet::new(&self.profile, shards)?;
        truncate_data_shards(self.reconstruct_data(&valid)?, original_size)
    }

    fn repair_shard(
        &self,
        shards: &[Option<Shard>],
        missing_slot: u8,
    ) -> Result<Shard, CodecError> {
        ensure_slot_in_profile(&self.profile, missing_slot)?;
        let valid = ValidShardSet::new(&self.profile, shards)?;
        ensure_slot_is_absent(&valid, missing_slot)?;

        // Inside the group first: XOR of the group's other members rebuilds a
        // data shard or a local parity shard without touching the rest of the
        // chunk. Only when a source is missing does the repair decode.
        let local_sources =
            Self::local_repair_sources(self.profile.k, self.local_groups, missing_slot)
                .map(|sources| {
                    sources
                        .into_iter()
                        .map(|slot| {
                            valid.present.iter().find(|candidate| candidate.slot_index == slot)
                        })
                        .collect::<Option<Vec<_>>>()
                })
                .unwrap_or_default();
        if let Some((first, rest)) = local_sources.as_deref().and_then(<[_]>::split_first) {
            let mut repaired = first.data.to_vec();
            for source in rest {
                if source.data.len() != repaired.len() {
                    return Err(CodecError::InvalidShardSet {
                        reason: "mixed shard sizes during local repair".to_string(),
                    });
                }
                for (idx, byte) in source.data.iter().enumerate() {
                    repaired[idx] ^= *byte;
                }
            }
            return Shard::new(
                missing_slot,
                valid.chunk_index,
                self.profile.role_for_slot(missing_slot)?,
                Bytes::from(repaired),
            );
        }

        // Outside the group, the unified solver expresses this one slot as a
        // single combination of `k` surviving shards. Decoding the whole chunk
        // and encoding the slot again would give the same bytes for `k` times
        // the byte work, on a path the repair worker runs continuously.
        self.ensure_solver_may_run()?;
        let repaired = self.solver.recover_slot(&valid.present, missing_slot)?;
        Shard::new(
            missing_slot,
            valid.chunk_index,
            self.profile.role_for_slot(missing_slot)?,
            Bytes::from(repaired),
        )
    }

    fn failure_matrix(&self) -> FailureMatrix {
        let mut entries = Vec::new();
        for group in 0..self.local_groups {
            let data_slot = group * self.data_per_group;
            entries.push(self.matrix_entry(
                format!("local-group-{group}-single-data"),
                vec![data_slot],
                format!(
                    "one missing data shard in local group {group} is repaired by XOR local parity"
                ),
            ));
            entries.push(self.matrix_entry(
                format!("local-group-{group}-local-parity"),
                vec![self.local_parity_slot(group)],
                format!("local parity for group {group} is recomputed from group data"),
            ));
        }
        entries.push(self.matrix_entry(
            "global-rs-budget".to_string(),
            (0..self.global_parity).collect(),
            format!(
                "RS global parity can recover {} missing data/global shard(s)",
                self.global_parity
            ),
        ));
        if self.data_per_group > self.global_parity {
            let mut missing_slots = self
                .data_slots_for_group(0)
                .take(usize::from(self.global_parity) + 1)
                .collect::<Vec<_>>();
            missing_slots.push(self.local_parity_slot(0));
            entries.push(self.matrix_entry(
                "same-group-data-plus-local-parity-beyond-global-budget".to_string(),
                missing_slots,
                "losing more same-group data shards than global parity can solve, plus the local parity shard, is not recoverable".to_string(),
            ));
        }

        // Patterns the unified solver reaches and the fast paths do not, one
        // per local group, appended after every version 1 entry. Dropping them
        // gives back the version 1 table exactly, which is how a half-upgraded
        // cluster stays comparable (decision EC2-103, section 3).
        //
        // Each one is deliberately the twin of the unrecoverable entry above:
        // the same data shards are gone, and the whole difference is that the
        // group's local parity survives. An operator reading the pair sees the
        // boundary as the single shard it is. Where a group holds no more data
        // shards than there is global parity, the solver adds nothing and the
        // table honestly stays at version 1.
        if self.data_per_group > self.global_parity && self.reach.solves() {
            for group in 0..self.local_groups {
                let missing_slots = self
                    .data_slots_for_group(group)
                    .take(usize::from(self.global_parity) + 1)
                    .collect::<Vec<_>>();
                entries.push(self.matrix_entry(
                    format!("local-group-{group}-data-beyond-global-budget-with-local-parity"),
                    missing_slots,
                    format!(
                        "{} data shard(s) of local group {group} are more than global parity alone carries; the unified solver reaches them through the group's surviving local parity",
                        usize::from(self.global_parity) + 1
                    ),
                ));
            }
        }

        FailureMatrix::new(&self.profile, entries)
    }

    /// The fast paths answer first; only what they leave over is put to the
    /// solver. Nothing is read off a pattern's name, because a name describes
    /// why a pattern is documented, not what it costs to read (decision
    /// EC2-103, section 2).
    fn recovery_path(&self, lost: &[bool]) -> RecoveryPath {
        let fast =
            Self::fast_path_after_loss(self.profile.k, self.local_groups, self.global_parity, lost);
        match fast {
            RecoveryPath::None if self.reach.solves() && self.solver.recovers_after_loss(lost) => {
                RecoveryPath::UnifiedSolver
            }
            path => path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::build_codec;

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
        (0..25_000).map(|idx| ((idx * 17) % 251) as u8).collect()
    }

    #[test]
    fn lrc_presets_decode_without_loss() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let codec = LrcCodec::new(profile).unwrap();
            let encoded = codec.encode_chunk(&payload(), 11).unwrap();
            let decoded = codec
                .decode_chunk(&options_without(&encoded, &[]), encoded.original_size)
                .unwrap();
            assert_eq!(decoded.as_ref(), payload().as_slice());
        }
    }

    #[test]
    fn lrc_repairs_single_data_shard_with_local_xor() {
        let codec = LrcCodec::new(EcProfileSnapshot::lrc_standard()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 2).unwrap();
        let repaired = codec.repair_shard(&options_without(&encoded, &[1]), 1).unwrap();
        assert_eq!(repaired, encoded.shards[1]);
        let decoded = codec
            .decode_chunk(&options_without(&encoded, &[1]), encoded.original_size)
            .unwrap();
        assert_eq!(decoded.as_ref(), payload().as_slice());
    }

    #[test]
    fn lrc_repairs_local_and_global_parity() {
        let codec = LrcCodec::new(EcProfileSnapshot::lrc_standard()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 5).unwrap();
        for missing_slot in [6, 9] {
            let repaired = codec
                .repair_shard(&options_without(&encoded, &[missing_slot]), missing_slot)
                .unwrap();
            assert_eq!(repaired, encoded.shards[usize::from(missing_slot)]);
        }
    }

    /// A local parity shard is the XOR of its group's data, so those data
    /// shards alone rebuild it — every other shard of the chunk may be gone.
    /// Before plan 05, phase 3 the codec decoded the whole chunk for it and
    /// needed `k` shards (EC2-085, point 3).
    #[test]
    fn lrc_rebuilds_local_parity_from_its_group_data_alone() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let codec = LrcCodec::new(profile.clone()).unwrap();
            let encoded = codec.encode_chunk(&payload(), 3).unwrap();
            for group in 0..codec.local_groups {
                let keep = codec.data_slots_for_group(group).collect::<Vec<_>>();
                let gone = (0..profile.total_shards())
                    .filter(|slot| !keep.contains(slot))
                    .collect::<Vec<_>>();
                let local_slot = codec.local_parity_slot(group);
                let repaired =
                    codec.repair_shard(&options_without(&encoded, &gone), local_slot).unwrap();
                assert_eq!(
                    repaired,
                    encoded.shards[usize::from(local_slot)],
                    "{}",
                    profile.name
                );
            }
        }
    }

    #[test]
    fn lrc_uses_global_rs_fallback_when_local_group_loses_two_data_shards() {
        let codec = LrcCodec::new(EcProfileSnapshot::lrc_standard()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 6).unwrap();
        let decoded = codec
            .decode_chunk(&options_without(&encoded, &[0, 1]), encoded.original_size)
            .unwrap();
        assert_eq!(decoded.as_ref(), payload().as_slice());
    }

    #[test]
    fn lrc_combines_local_repair_with_global_rs_fallback() {
        let codec = LrcCodec::new(EcProfileSnapshot::lrc_standard()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 7).unwrap();
        let decoded = codec
            .decode_chunk(
                &options_without(&encoded, &[0, 1, 3]),
                encoded.original_size,
            )
            .unwrap();
        assert_eq!(decoded.as_ref(), payload().as_slice());
    }

    #[test]
    fn lrc_failure_matrix_patterns_match_codec_behavior() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let codec = LrcCodec::new(profile).unwrap();
            let encoded = codec.encode_chunk(&payload(), 8).unwrap();
            for entry in codec.failure_matrix().entries {
                let result = codec.decode_chunk(
                    &options_without(&encoded, &entry.missing_slots),
                    encoded.original_size,
                );
                assert_eq!(
                    result.is_ok(),
                    entry.recoverable,
                    "pattern {} failed expectation",
                    entry.label
                );
            }
        }
    }

    #[test]
    fn lrc_failure_matrix_skips_false_nonrecoverable_entry_for_small_groups() {
        let profile = EcProfileSnapshot::new_lrc("custom-small-groups", 4, 4, 2).unwrap();
        let codec = LrcCodec::new(profile).unwrap();
        let encoded = codec.encode_chunk(&payload(), 12).unwrap();

        assert!(!codec
            .failure_matrix()
            .entries
            .iter()
            .any(|entry| entry.label == "same-group-data-plus-local-parity-beyond-global-budget"));

        for entry in codec.failure_matrix().entries {
            let result = codec.decode_chunk(
                &options_without(&encoded, &entry.missing_slots),
                encoded.original_size,
            );
            assert_eq!(
                result.is_ok(),
                entry.recoverable,
                "pattern {} failed expectation",
                entry.label
            );
        }
    }

    /// `EC2-LRC-03-001`. The patterns EC2-101 counted as the gain: a whole
    /// local group's data past what global parity alone carries, with the
    /// group's local parity still alive. EC v1 refused every one of these.
    #[test]
    fn a_group_past_the_global_budget_now_decodes() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let codec = LrcCodec::new(profile.clone()).unwrap();
            let encoded = codec.encode_chunk(&payload(), 21).unwrap();
            for group in 0..codec.local_groups {
                let start = group * codec.data_per_group;
                let missing = (start..=start + codec.global_parity).collect::<Vec<u8>>();

                // Not a gain unless the fast paths really do refuse it.
                let lost = (0..profile.total_shards())
                    .map(|slot| missing.contains(&slot))
                    .collect::<Vec<_>>();
                assert!(
                    !LrcCodec::fast_path_after_loss(
                        profile.k,
                        codec.local_groups,
                        codec.global_parity,
                        &lost
                    )
                    .recovers(),
                    "{} {missing:?} is already on a fast path",
                    profile.name
                );

                let decoded = codec
                    .decode_chunk(&options_without(&encoded, &missing), encoded.original_size)
                    .unwrap();
                assert_eq!(
                    decoded.as_ref(),
                    payload().as_slice(),
                    "{} {missing:?}",
                    profile.name
                );
            }
        }
    }

    /// `EC2-LRC-03-002`. A shard rebuilt through the solver is the encoder's
    /// own shard — the bytes, and with them the CRC32, the SHA-256, the role
    /// and the chunk index that travel with it. Anything less would store a
    /// shard that only looks consistent, because a freshly computed checksum
    /// agrees with whatever bytes it was handed.
    #[test]
    fn a_shard_the_solver_repairs_is_the_encoders_own_shard() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let codec = LrcCodec::new(profile.clone()).unwrap();
            let encoded = codec.encode_chunk(&payload(), 22).unwrap();
            let missing = (0..=codec.global_parity).collect::<Vec<u8>>();
            let available = options_without(&encoded, &missing);

            for slot in &missing {
                let repaired = codec.repair_shard(&available, *slot).unwrap();
                assert_eq!(
                    repaired,
                    encoded.shards[usize::from(*slot)],
                    "{} slot {slot}",
                    profile.name
                );
            }
        }
    }

    /// `EC2-LRC-03-003`. The solver is the fallback, and this holds it to
    /// that: over every loss pattern of every standard profile, a decode the
    /// fast paths handle runs no unified solve at all, and one they cannot
    /// handle runs exactly one — success or refusal. The same for every repair
    /// of every slot each pattern took away, where the fast path is the XOR
    /// inside the local group.
    ///
    /// This is what keeps the ordinary read at the speed it had: not a comment
    /// promising the fallback stays behind, but a count of the times it ran.
    #[test]
    fn the_fast_paths_never_reach_the_solver() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let codec = LrcCodec::new(profile.clone()).unwrap();
            let (k, local, global) = (profile.k, codec.local_groups, codec.global_parity);
            let n = usize::from(profile.total_shards());
            // Four bytes per shard: what is under test is which path runs, not
            // the arithmetic on long buffers, and `lrc_dense` alone has 16 384
            // patterns.
            let input =
                (0..4 * u32::from(k)).map(|idx| ((idx * 7) % 251) as u8).collect::<Vec<_>>();
            let encoded = codec.encode_chunk(&input, 23).unwrap();

            // Every mask but the last: with nothing left the shard set is
            // refused before any recovery begins.
            for mask in 0u32..((1 << n) - 1) {
                let lost = (0..n).map(|slot| mask & (1 << slot) != 0).collect::<Vec<_>>();
                let missing = (0..n)
                    .filter(|slot| lost[*slot])
                    .map(|slot| u8::try_from(slot).unwrap())
                    .collect::<Vec<_>>();
                let shards = options_without(&encoded, &missing);

                let fast = LrcCodec::fast_path_after_loss(k, local, global, &lost);
                let before = LrcSolver::solves_on_this_thread();
                let decoded = codec.decode_chunk(&shards, encoded.original_size);
                assert_eq!(
                    LrcSolver::solves_on_this_thread() - before,
                    usize::from(!fast.recovers()),
                    "{} mask {mask:#b}: decode took the wrong path",
                    profile.name
                );
                if fast.recovers() {
                    assert_eq!(
                        decoded.unwrap().as_ref(),
                        input.as_slice(),
                        "{} mask {mask:#b}",
                        profile.name
                    );
                }

                for slot in &missing {
                    let local_sources = LrcCodec::local_repair_sources(k, local, *slot)
                        .is_some_and(|sources| {
                            sources.iter().all(|source| !lost[usize::from(*source)])
                        });
                    let before = LrcSolver::solves_on_this_thread();
                    let repaired = codec.repair_shard(&shards, *slot);
                    assert_eq!(
                        LrcSolver::solves_on_this_thread() - before,
                        usize::from(!local_sources),
                        "{} mask {mask:#b} slot {slot}: repair took the wrong path",
                        profile.name
                    );
                    if local_sources {
                        assert_eq!(
                            repaired.unwrap(),
                            encoded.shards[usize::from(*slot)],
                            "{} mask {mask:#b} slot {slot}",
                            profile.name
                        );
                    }
                }
            }
        }
    }

    /// The version 1 failure matrix of one standard profile: every label, its
    /// slots and its verdict in the order EC v1 published them, together with
    /// the path EC2-103 works out for each by hand.
    ///
    /// Recorded rather than recomputed. A version 2 table has to carry a
    /// version 1 table unchanged, and an expectation rebuilt from the same code
    /// would follow any drift instead of catching it.
    type MatrixRow = (&'static str, &'static [u8], bool, RecoveryPath);

    const LRC_SMALL_V1_TABLE: &[MatrixRow] = &[
        (
            "local-group-0-single-data",
            &[0],
            true,
            RecoveryPath::LocalXor,
        ),
        (
            "local-group-0-local-parity",
            &[4],
            true,
            RecoveryPath::Direct,
        ),
        (
            "local-group-1-single-data",
            &[2],
            true,
            RecoveryPath::LocalXor,
        ),
        (
            "local-group-1-local-parity",
            &[5],
            true,
            RecoveryPath::Direct,
        ),
        // Named after the global budget, but a single lost data shard of
        // `lrc_small` is repaired inside its group: the path is computed, so
        // the document says XOR and not RS (EC2-103, section 2).
        ("global-rs-budget", &[0], true, RecoveryPath::LocalXor),
        (
            "same-group-data-plus-local-parity-beyond-global-budget",
            &[0, 1, 4],
            false,
            RecoveryPath::None,
        ),
    ];
    const LRC_STANDARD_V1_TABLE: &[MatrixRow] = &[
        (
            "local-group-0-single-data",
            &[0],
            true,
            RecoveryPath::LocalXor,
        ),
        (
            "local-group-0-local-parity",
            &[6],
            true,
            RecoveryPath::Direct,
        ),
        (
            "local-group-1-single-data",
            &[3],
            true,
            RecoveryPath::LocalXor,
        ),
        (
            "local-group-1-local-parity",
            &[7],
            true,
            RecoveryPath::Direct,
        ),
        ("global-rs-budget", &[0, 1], true, RecoveryPath::GlobalRs),
        (
            "same-group-data-plus-local-parity-beyond-global-budget",
            &[0, 1, 2, 6],
            false,
            RecoveryPath::None,
        ),
    ];
    const LRC_DENSE_V1_TABLE: &[MatrixRow] = &[
        (
            "local-group-0-single-data",
            &[0],
            true,
            RecoveryPath::LocalXor,
        ),
        (
            "local-group-0-local-parity",
            &[10],
            true,
            RecoveryPath::Direct,
        ),
        (
            "local-group-1-single-data",
            &[5],
            true,
            RecoveryPath::LocalXor,
        ),
        (
            "local-group-1-local-parity",
            &[11],
            true,
            RecoveryPath::Direct,
        ),
        ("global-rs-budget", &[0, 1], true, RecoveryPath::GlobalRs),
        (
            "same-group-data-plus-local-parity-beyond-global-budget",
            &[0, 1, 2, 10],
            false,
            RecoveryPath::None,
        ),
    ];

    /// The slots EC2-103 lists for the entries the solver adds, one per local
    /// group: the first `g + 1` data shards of the group, its local parity
    /// left alive.
    const SOLVER_ENTRY_SLOTS: &[(&str, &[&[u8]])] = &[
        ("ec-lrc-small", &[&[0, 1], &[2, 3]]),
        ("ec-lrc-standard", &[&[0, 1, 2], &[3, 4, 5]]),
        ("ec-lrc-dense", &[&[0, 1, 2], &[5, 6, 7]]),
    ];

    /// `EC2-LRC-03-004`. The document an operator reads: the version 1 table
    /// comes through untouched — same labels, same slots, same verdicts, same
    /// order — the solver's patterns follow it, and the table says version 2.
    ///
    /// The two halves matter together. Dropping every `unified_solver` entry
    /// has to give back the version 1 table exactly, because that is what makes
    /// the difference between a node of each version an addition rather than a
    /// disagreement while a cluster is half upgraded.
    #[test]
    fn the_version_1_table_survives_and_the_solver_entries_follow_it() {
        for (profile, recorded) in [
            (EcProfileSnapshot::lrc_small(), LRC_SMALL_V1_TABLE),
            (EcProfileSnapshot::lrc_standard(), LRC_STANDARD_V1_TABLE),
            (EcProfileSnapshot::lrc_dense(), LRC_DENSE_V1_TABLE),
        ] {
            let codec = LrcCodec::new(profile.clone()).unwrap();
            let matrix = codec.failure_matrix();
            let name = &profile.name;
            assert_eq!(
                matrix.version,
                FailureMatrix::VERSION_UNIFIED_SOLVER,
                "{name}"
            );

            let (fast, solved): (Vec<_>, Vec<_>) = matrix
                .entries
                .iter()
                .partition(|entry| entry.recovery_path != RecoveryPath::UnifiedSolver);

            assert_eq!(fast.len(), recorded.len(), "{name}");
            for (entry, (label, slots, recoverable, path)) in fast.iter().zip(recorded) {
                assert_eq!(entry.label, *label, "{name}");
                assert_eq!(entry.missing_slots, *slots, "{name} {label}");
                assert_eq!(entry.recoverable, *recoverable, "{name} {label}");
                assert_eq!(entry.recovery_path, *path, "{name} {label}");
            }
            // The version 1 entries come first, so dropping the solver's
            // entries leaves the earlier table in its own order.
            assert_eq!(
                matrix.entries[..fast.len()].iter().collect::<Vec<_>>(),
                fast,
                "{name}: the solver's entries are not appended last"
            );

            let expected_slots = SOLVER_ENTRY_SLOTS
                .iter()
                .find(|(candidate, _)| candidate == name)
                .map(|(_, slots)| *slots)
                .unwrap_or_else(|| panic!("{name} has no recorded solver entries"));
            assert_eq!(solved.len(), expected_slots.len(), "{name}");
            for (group, (entry, slots)) in solved.iter().zip(expected_slots).enumerate() {
                assert_eq!(
                    entry.label,
                    format!("local-group-{group}-data-beyond-global-budget-with-local-parity"),
                    "{name}"
                );
                assert_eq!(entry.missing_slots, *slots, "{name} group {group}");
                assert!(entry.recoverable, "{name} group {group}");
            }
        }
    }

    /// A profile the solver cannot help keeps version 1, and says so. In
    /// `LRC(4,4,2)` a group holds one data shard against two global parity
    /// shards, so there is never an equation the fast paths leave unused, and
    /// EC2-101 measured the gain at exactly zero patterns.
    #[test]
    fn a_profile_the_solver_cannot_help_stays_at_version_1() {
        for profile in [
            EcProfileSnapshot::new_lrc("custom-small-groups", 4, 4, 2).unwrap(),
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
        ] {
            let codec = build_codec(profile.clone()).unwrap();
            let matrix = codec.failure_matrix();
            assert_eq!(
                matrix.version,
                FailureMatrix::VERSION_FAST_PATH,
                "{}",
                profile.name
            );
            assert!(
                matrix
                    .entries
                    .iter()
                    .all(|entry| entry.recovery_path != RecoveryPath::UnifiedSolver),
                "{}",
                profile.name
            );
        }
    }

    /// `EC2-LRC-03-004`. The path each entry declares is the path the codec
    /// takes, measured rather than believed: an entry on a fast path decodes
    /// without the solver being reached at all, an entry on the solver reaches
    /// it exactly once, and an unrecoverable entry fails.
    ///
    /// This is what makes `recovery_path` a statement about cost an operator
    /// can act on, instead of a word beside a pattern.
    #[test]
    fn every_documented_path_is_the_path_the_codec_takes() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
            EcProfileSnapshot::new_lrc("custom-small-groups", 4, 4, 2).unwrap(),
        ] {
            let codec = LrcCodec::new(profile.clone()).unwrap();
            let encoded = codec.encode_chunk(&payload(), 24).unwrap();

            for entry in codec.failure_matrix().entries {
                let shards = options_without(&encoded, &entry.missing_slots);
                let before = LrcSolver::solves_on_this_thread();
                let decoded = codec.decode_chunk(&shards, encoded.original_size);
                let solves = LrcSolver::solves_on_this_thread() - before;

                // An unrecoverable pattern reaches the solver too, and is
                // refused there: only a fast path keeps the solver out of the
                // way, which is the whole claim being measured.
                let on_a_fast_path = matches!(
                    entry.recovery_path,
                    RecoveryPath::Direct | RecoveryPath::LocalXor | RecoveryPath::GlobalRs
                );
                let expected_solves = usize::from(!on_a_fast_path);
                assert_eq!(
                    solves, expected_solves,
                    "{} pattern {} claims {:?}",
                    profile.name, entry.label, entry.recovery_path
                );
                assert_eq!(
                    decoded.is_ok(),
                    entry.recoverable,
                    "{} pattern {}",
                    profile.name,
                    entry.label
                );
                if let Ok(bytes) = decoded {
                    assert_eq!(
                        bytes.as_ref(),
                        payload().as_slice(),
                        "{} pattern {}",
                        profile.name,
                        entry.label
                    );
                }
            }
        }
    }

    #[test]
    fn lrc_rejects_wrong_hash_and_role() {
        let codec = LrcCodec::new(EcProfileSnapshot::lrc_small()).unwrap();
        let encoded = codec.encode_chunk(&payload(), 4).unwrap();

        let mut shards = options_without(&encoded, &[]);
        shards[0].as_mut().unwrap().sha256 = [1; 32];
        let err = codec.decode_chunk(&shards, encoded.original_size).unwrap_err();
        assert!(err.to_string().contains("sha256"));

        let mut shards = options_without(&encoded, &[]);
        shards[0].as_mut().unwrap().role = ShardRole::LrcLocalParity;
        let err = codec.decode_chunk(&shards, encoded.original_size).unwrap_err();
        assert!(err.to_string().contains("expected"));
    }

    // The bytes `encode_chunk` produces for the standard LRC profiles, taken
    // from the encoder on 2026-09-12, at the end of plan 06 phase 2, while
    // `lrc.rs` was still untouched by that plan. Eight bytes per data shard of
    // the payload EC2-100 verified its matrix against.
    const LRC_SMALL_REFERENCE: &[&[u8]] = &[
        &[11, 48, 85, 122, 159, 196, 233, 19],
        &[56, 93, 130, 167, 204, 241, 27, 64],
        &[101, 138, 175, 212, 249, 35, 72, 109],
        &[146, 183, 220, 6, 43, 80, 117, 154],
        &[51, 109, 215, 221, 83, 53, 242, 83],
        &[247, 61, 115, 210, 210, 115, 61, 247],
        &[39, 146, 147, 200, 7, 178, 168, 45],
    ];
    const LRC_STANDARD_REFERENCE: &[&[u8]] = &[
        &[11, 48, 85, 122, 159, 196, 233, 19],
        &[56, 93, 130, 167, 204, 241, 27, 64],
        &[101, 138, 175, 212, 249, 35, 72, 109],
        &[146, 183, 220, 6, 43, 80, 117, 154],
        &[191, 228, 14, 51, 88, 125, 162, 199],
        &[236, 22, 59, 96, 133, 170, 207, 244],
        &[86, 231, 120, 9, 170, 22, 186, 62],
        &[193, 69, 233, 85, 246, 135, 24, 169],
        &[142, 71, 164, 38, 133, 202, 26, 230],
        &[174, 153, 189, 122, 52, 61, 156, 185],
    ];
    const LRC_DENSE_REFERENCE: &[&[u8]] = &[
        &[11, 48, 85, 122, 159, 196, 233, 19],
        &[56, 93, 130, 167, 204, 241, 27, 64],
        &[101, 138, 175, 212, 249, 35, 72, 109],
        &[146, 183, 220, 6, 43, 80, 117, 154],
        &[191, 228, 14, 51, 88, 125, 162, 199],
        &[236, 22, 59, 96, 133, 170, 207, 244],
        &[30, 67, 104, 141, 178, 215, 1, 38],
        &[75, 112, 149, 186, 223, 9, 46, 83],
        &[120, 157, 194, 231, 17, 54, 91, 128],
        &[165, 202, 239, 25, 62, 99, 136, 173],
        &[123, 180, 170, 60, 217, 59, 109, 99],
        &[100, 114, 235, 169, 199, 33, 51, 172],
        &[119, 19, 8, 140, 42, 170, 183, 49],
        &[124, 54, 142, 97, 240, 133, 249, 178],
    ];

    /// `EC2-LRC-03-005`. Encoding is the storage format, and plan 06 changes
    /// recovery only: every shard already on disk has to stay valid, so
    /// `encode_chunk` must keep producing the same bytes it always has.
    ///
    /// The reference values above were recorded before the plan touched this
    /// file, which is the whole point — a test that compared the encoder with
    /// itself would pass just as happily after the format moved. A failure here
    /// is a format change and a breach of section 5 of the EC v2 conventions,
    /// not a test to bring up to date.
    #[test]
    fn encoding_matches_the_recorded_reference_vectors() {
        for (profile, reference) in [
            (EcProfileSnapshot::lrc_small(), LRC_SMALL_REFERENCE),
            (EcProfileSnapshot::lrc_standard(), LRC_STANDARD_REFERENCE),
            (EcProfileSnapshot::lrc_dense(), LRC_DENSE_REFERENCE),
        ] {
            let codec = LrcCodec::new(profile.clone()).unwrap();
            let input = (0..8u32 * u32::from(profile.k))
                .map(|idx| ((idx * 37 + 11) % 251) as u8)
                .collect::<Vec<_>>();
            let encoded = codec.encode_chunk(&input, 0).unwrap();

            assert_eq!(encoded.shard_size, 8, "{}", profile.name);
            assert_eq!(encoded.shards.len(), reference.len(), "{}", profile.name);
            for (shard, expected) in encoded.shards.iter().zip(reference) {
                assert_eq!(
                    shard.data.as_ref(),
                    *expected,
                    "{} slot {} moved: encoding is the storage format",
                    profile.name,
                    shard.slot_index
                );
            }
        }
    }

    /// The slots of a profile's solver-only pattern: one more data shard of
    /// local group 0 than global parity alone carries, with the group's local
    /// parity still alive. This is the pattern EC2-103 documents as
    /// `local-group-0-data-beyond-global-budget-with-local-parity`, and the
    /// boundary the two editions of the decoder disagree about.
    fn solver_only_pattern(profile: &EcProfileSnapshot) -> Vec<u8> {
        let EcParity::Lrc { local, global } = profile.parity else {
            panic!("{} is not an LRC profile", profile.name);
        };
        assert!(
            profile.k / local > global,
            "{} gains nothing from the solver",
            profile.name
        );
        (0..=global).collect()
    }

    /// A short payload for a sweep over every loss pattern: long enough that a
    /// wrong byte has somewhere to hide, short enough to decode 16 384 times.
    fn sweep_payload(profile: &EcProfileSnapshot) -> Vec<u8> {
        (0..64u32 * u32::from(profile.k))
            .map(|idx| ((idx * 31 + 7) % 251) as u8)
            .collect()
    }

    /// `EC2-LRC-05-004`. A node held to the fast paths is the EC v1 decoder,
    /// pattern for pattern — not approximately, and not only on the patterns
    /// someone thought to list.
    ///
    /// This is what makes the mixed-cluster script honest: one build has to be
    /// able to stand in for both sides of a rolling upgrade, and a stand-in
    /// that recovered a little more or a little less than EC v1 would prove
    /// something about itself rather than about the upgrade. The count of
    /// patterns it refuses is the fingerprint the two-stage decoder left
    /// before plan 06 — 80, 765 and 15 673.
    #[test]
    fn a_node_on_the_fast_paths_alone_is_the_ec_v1_decoder() {
        for (profile, ec_v1_refusals) in [
            (EcProfileSnapshot::lrc_small(), 80),
            (EcProfileSnapshot::lrc_standard(), 765),
            (EcProfileSnapshot::lrc_dense(), 15_673),
        ] {
            let unified = LrcCodec::new(profile.clone()).unwrap();
            let pre_solver =
                LrcCodec::with_reach(profile.clone(), RecoveryReach::FastPathsOnly).unwrap();
            let input = sweep_payload(&profile);
            let encoded = unified.encode_chunk(&input, 0).unwrap();
            let slots = usize::from(profile.total_shards());
            let mut refused = 0usize;

            for mask in 0u32..(1 << slots) {
                let lost = (0..slots).map(|slot| mask & (1 << slot) != 0).collect::<Vec<_>>();
                let missing = (0..slots)
                    .filter(|slot| lost[*slot])
                    .map(|slot| u8::try_from(slot).unwrap())
                    .collect::<Vec<_>>();
                let shards = options_without(&encoded, &missing);

                let path = pre_solver.recovery_path(&lost);
                assert_ne!(
                    path,
                    RecoveryPath::UnifiedSolver,
                    "{} mask {mask:#b}: a pre-solver node cannot take a path it does not have",
                    profile.name
                );
                assert_eq!(
                    path,
                    LrcCodec::fast_path_after_loss(
                        profile.k,
                        local_of(&profile),
                        global_of(&profile),
                        &lost
                    ),
                    "{} mask {mask:#b}",
                    profile.name
                );

                match pre_solver.decode_chunk(&shards, encoded.original_size) {
                    Ok(decoded) => {
                        assert!(path.recovers(), "{} mask {mask:#b}", profile.name);
                        assert_eq!(
                            decoded.as_ref(),
                            input.as_slice(),
                            "{} mask {mask:#b}",
                            profile.name
                        );
                        // Whatever EC v1 reached, the solver reaches too, and
                        // reaches it with the same bytes.
                        let by_solver =
                            unified.decode_chunk(&shards, encoded.original_size).unwrap();
                        assert_eq!(by_solver, decoded, "{} mask {mask:#b}", profile.name);
                    }
                    Err(CodecError::NotRecoverable { .. }) => {
                        assert!(!path.recovers(), "{} mask {mask:#b}", profile.name);
                        refused += 1;
                    }
                    Err(other) => panic!("{} mask {mask:#b}: unexpected {other}", profile.name),
                }
            }

            assert_eq!(
                refused, ec_v1_refusals,
                "{} no longer refuses what the two-stage decoder refused",
                profile.name
            );
        }
    }

    /// The local group count of an LRC profile, for the few places that need
    /// the shape rather than the codec.
    fn local_of(profile: &EcProfileSnapshot) -> u8 {
        match profile.parity {
            EcParity::Lrc { local, .. } => local,
            EcParity::ReedSolomon { .. } => panic!("{} is not an LRC profile", profile.name),
        }
    }

    /// The global parity count of an LRC profile.
    fn global_of(profile: &EcProfileSnapshot) -> u8 {
        match profile.parity {
            EcParity::Lrc { global, .. } => global,
            EcParity::ReedSolomon { .. } => panic!("{} is not an LRC profile", profile.name),
        }
    }

    /// `EC2-103`, section 3. The document a pre-solver node publishes is the
    /// document a solver node publishes with the solver-only entries dropped —
    /// the same labels, slots, verdicts and order, and a version that says so.
    ///
    /// Operators compare this document between nodes byte for byte while a
    /// cluster is half upgraded, so the difference has to be an addition and
    /// nothing else.
    #[test]
    fn a_node_on_the_fast_paths_alone_publishes_the_version_1_matrix() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let with_solver = LrcCodec::new(profile.clone()).unwrap().failure_matrix();
            let pre_solver = LrcCodec::with_reach(profile.clone(), RecoveryReach::FastPathsOnly)
                .unwrap()
                .failure_matrix();

            assert_eq!(
                with_solver.version,
                FailureMatrix::VERSION_UNIFIED_SOLVER,
                "{}",
                profile.name
            );
            assert_eq!(
                pre_solver.version,
                FailureMatrix::VERSION_FAST_PATH,
                "{}",
                profile.name
            );

            let dropped = with_solver
                .entries
                .iter()
                .filter(|entry| entry.recovery_path != RecoveryPath::UnifiedSolver)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(pre_solver.entries, dropped, "{}", profile.name);
            assert!(
                dropped.len() < with_solver.entries.len(),
                "{} documents no solver-only pattern at all",
                profile.name
            );

            // Everything outside the entry list describes the profile, not the
            // decoder, so the two documents must agree on all of it.
            assert_eq!(pre_solver.profile_name, with_solver.profile_name);
            assert_eq!(pre_solver.codec, with_solver.codec);
            assert_eq!(pre_solver.k, with_solver.k);
            assert_eq!(pre_solver.total_shards, with_solver.total_shards);
        }
    }

    /// Holding recovery back must not touch the write path: the two editions
    /// write the same bytes, which is why a half-upgraded cluster is safe and
    /// why the hook is safe to arm on a node that also takes writes
    /// (decision EC2-103, section 7).
    #[test]
    fn a_node_on_the_fast_paths_alone_writes_the_same_shards() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let unified = LrcCodec::new(profile.clone()).unwrap();
            let pre_solver =
                LrcCodec::with_reach(profile.clone(), RecoveryReach::FastPathsOnly).unwrap();
            assert_eq!(
                pre_solver.encode_chunk(&payload(), 7).unwrap(),
                unified.encode_chunk(&payload(), 7).unwrap(),
                "{}",
                profile.name
            );
        }
    }

    /// `EC2-LRC-05-004`, both directions, at the codec level the acceptance
    /// script cannot reach.
    ///
    /// Forward: a shard rebuilt by a solver node is the encoder's own shard,
    /// so a pre-solver node takes it without noticing anything happened — and
    /// once it is back, that node reads the chunk on its fast paths.
    /// Backward: the pattern that needed the solver is, to a pre-solver node, a
    /// refusal to read. Not corrupt bytes, not a plausible answer — the same
    /// `NotRecoverable` it returned before plan 06 existed.
    #[test]
    fn a_pre_solver_node_takes_the_shard_a_solver_node_rebuilt() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let unified = LrcCodec::new(profile.clone()).unwrap();
            let pre_solver =
                LrcCodec::with_reach(profile.clone(), RecoveryReach::FastPathsOnly).unwrap();
            let encoded = unified.encode_chunk(&payload(), 4).unwrap();
            let missing = solver_only_pattern(&profile);
            let degraded = options_without(&encoded, &missing);

            assert!(
                matches!(
                    pre_solver.decode_chunk(&degraded, encoded.original_size),
                    Err(CodecError::NotRecoverable { .. })
                ),
                "{}: a pre-solver node must refuse, not answer",
                profile.name
            );
            assert!(
                matches!(
                    pre_solver.repair_shard(&degraded, missing[0]),
                    Err(CodecError::NotRecoverable { .. })
                ),
                "{}: a pre-solver node must refuse the repair too",
                profile.name
            );

            let mut healed = degraded.clone();
            for slot in &missing {
                let repaired = unified.repair_shard(&degraded, *slot).unwrap();
                assert_eq!(
                    repaired,
                    encoded.shards[usize::from(*slot)],
                    "{} slot {slot}: a rebuilt shard must be the encoder's shard",
                    profile.name
                );
                healed[usize::from(*slot)] = Some(repaired);
            }

            let lost_nothing = vec![false; usize::from(profile.total_shards())];
            assert_eq!(
                pre_solver.recovery_path(&lost_nothing),
                RecoveryPath::Direct,
                "{}",
                profile.name
            );
            assert_eq!(
                pre_solver.decode_chunk(&healed, encoded.original_size).unwrap().as_ref(),
                payload().as_slice(),
                "{}: the healed chunk has to read on the fast paths alone",
                profile.name
            );
        }
    }

    /// Plan 06 phase 4: the codec held against theory it shares no code with.
    ///
    /// The tests above compare the codec with records — the fingerprint
    /// EC2-102 wrote down, the reference shards of EC v1, the two-stage model
    /// of the fast paths. This module computes the answer instead: for every
    /// loss pattern, whether the data is determined at all. The answer is
    /// derived from the encoder and from `gf::invert_matrix`, which has been in
    /// the crate since EC v1 and knows nothing about the solver, so agreement
    /// between the two says something about the solver rather than repeating
    /// it (decision EC2-102).
    ///
    /// What has to be ruled out is not a refusal to recover — that costs
    /// availability and is visible — but its opposite: a success that returns
    /// rubbish. Rubbish passes every check further down, because a shard's
    /// CRC32 and SHA-256 are computed over whatever bytes it was handed; see
    /// `a_shards_own_checksum_cannot_tell_recovered_data_from_rubbish`. Only a
    /// comparison against the original payload catches it, and every success
    /// below goes through one.
    mod exhaustive {
        use super::*;
        use crate::gf;

        /// One way a chunk can be damaged: which of its slots are gone.
        ///
        /// The mask travels with the flags because it names the pattern in
        /// every failure message, and because the fingerprints of EC2-102 are
        /// sums over exactly these masks.
        struct LossPattern {
            mask: u32,
            lost: Vec<bool>,
        }

        impl LossPattern {
            /// The pattern in which exactly the slots marked in `lost` are
            /// gone.
            fn new(lost: Vec<bool>) -> Self {
                let mask = lost
                    .iter()
                    .enumerate()
                    .filter(|(_, gone)| **gone)
                    .map(|(slot, _)| 1u32 << slot)
                    .sum();
                Self { mask, lost }
            }

            /// The lost slots in ascending order, the form the codec's callers
            /// use.
            fn missing_slots(&self) -> Vec<u8> {
                self.lost
                    .iter()
                    .enumerate()
                    .filter(|(_, lost)| **lost)
                    .map(|(slot, _)| u8::try_from(slot).expect("a slot index fits in u8"))
                    .collect()
            }

            fn lost_count(&self) -> usize {
                self.lost.iter().filter(|lost| **lost).count()
            }
        }

        /// `EC2-LRC-04-001`. Every pattern of unavailable slots a profile can
        /// be in: one per subset of its `n` slots, so 2^n in all — 128, 1 024
        /// and 16 384 for the three standard LRC profiles.
        ///
        /// Slot `slot` is gone exactly when bit `slot` of the mask is set, so
        /// counting up through the masks walks every subset once and in an
        /// order any failure can be reproduced from.
        fn every_loss_pattern(slots: u8) -> impl Iterator<Item = LossPattern> {
            let slots = usize::from(slots);
            (0u32..1 << slots).map(move |mask| {
                LossPattern::new((0..slots).map(|slot| mask & (1 << slot) != 0).collect())
            })
        }

        fn binomial(n: usize, k: usize) -> usize {
            (0..k).fold(1, |acc, index| acc * (n - index) / (index + 1))
        }

        /// The unified matrix of a profile, read off the encoder instead of
        /// built beside it.
        ///
        /// Encoding is linear over GF(2^8) and works one byte position at a
        /// time, so a payload of `k` bytes gives every slot a one-byte shard
        /// holding `M[slot] · d`. Hand the encoder the `i`-th basis vector and
        /// every slot answers with `M[slot][i]`: column `i` comes back from the
        /// encoder itself. Nothing here restates how the matrix is put
        /// together, which is the point — a mistake in that construction has
        /// nothing to cancel against.
        fn matrix_from_encoder(codec: &LrcCodec) -> Vec<Vec<u8>> {
            let k = usize::from(codec.profile.k);
            let mut matrix = vec![vec![0; k]; usize::from(codec.profile.total_shards())];
            for column in 0..k {
                let mut basis = vec![0; k];
                basis[column] = 1;
                let encoded = codec.encode_chunk(&basis, 0).unwrap();
                assert_eq!(encoded.shard_size, 1, "one data byte per shard");
                for shard in &encoded.shards {
                    matrix[usize::from(shard.slot_index)][column] = shard.data[0];
                }
            }
            matrix
        }

        /// One equation applied to the data shards: the bytes it claims the
        /// slot holds.
        ///
        /// Spelled out over `gf::mul` and `gf::add` rather than reusing
        /// `rs::combine`, so that checking the matrix against the encoder
        /// borrows nothing from the code that recovers by it.
        fn evaluate(row: &[u8], data_shards: &[Vec<u8>]) -> Vec<u8> {
            let width = data_shards.first().map_or(0, Vec::len);
            (0..width)
                .map(|position| {
                    row.iter().zip(data_shards).fold(0, |acc, (coefficient, shard)| {
                        gf::add(acc, gf::mul(*coefficient, shard[position]))
                    })
                })
                .collect()
        }

        /// The theoretical verdict on one loss pattern, and what reaching it
        /// cost.
        struct Verdict {
            /// Whether the `k` data bytes are determined by what survives.
            recoverable: bool,
            /// Matrix inversions the oracle performed, which is what EC2-102
            /// measured when it decided the oracle was cheap enough to run
            /// over every pattern of every profile.
            inversions: usize,
        }

        /// Theory for one loss pattern, computed without a line of the solver:
        /// the data is determined exactly when some `k` of the surviving rows
        /// form an invertible matrix (decision EC2-102).
        ///
        /// The solver answers the same question by reducing the whole set of
        /// surviving rows to row echelon form and reading off its rank. This
        /// goes the other way: every `k`-subset through `gf::invert_matrix`,
        /// the inverter Reed-Solomon decoding has used since EC v1. The solver
        /// calls that inverter too, but only on a block whose rows it has
        /// already accepted — how each side reaches its verdict is shared
        /// nowhere.
        ///
        /// Subsets are tried in ascending order and the search stops at the
        /// first invertible one, which is why the whole sweep costs so little:
        /// wherever data shards survive, the identity rows come first.
        fn oracle(matrix: &[Vec<u8>], k: usize, lost: &[bool]) -> Verdict {
            let available = matrix
                .iter()
                .enumerate()
                .filter(|(slot, _)| !lost[*slot])
                .map(|(_, row)| row.clone())
                .collect::<Vec<_>>();
            let mut verdict = Verdict {
                recoverable: false,
                inversions: 0,
            };
            if available.len() < k {
                return verdict;
            }

            let mut chosen = (0..k).collect::<Vec<_>>();
            loop {
                let block = chosen.iter().map(|row| available[*row].clone()).collect::<Vec<_>>();
                verdict.inversions += 1;
                if gf::invert_matrix(&block).is_ok() {
                    verdict.recoverable = true;
                    return verdict;
                }
                if !next_subset(&mut chosen, available.len()) {
                    return verdict;
                }
            }
        }

        /// Advances `chosen` to the next ascending subset of `total` rows, or
        /// reports that it was already the last one.
        fn next_subset(chosen: &mut [usize], total: usize) -> bool {
            let size = chosen.len();
            for position in (0..size).rev() {
                if chosen[position] + size - position < total {
                    chosen[position] += 1;
                    for later in position + 1..size {
                        chosen[later] = chosen[later - 1] + 1;
                    }
                    return true;
                }
            }
            false
        }

        /// The slots a repair of `slot` reads inside its own local group.
        ///
        /// Stated here rather than taken from `LrcCodec::local_repair_sources`,
        /// so the prediction of which path a repair takes is the test's own:
        /// the rest of the group's data plus its local parity for a data slot,
        /// the group's data for a local parity slot, and no local path at all
        /// for a global parity slot.
        fn group_sources(codec: &LrcCodec, slot: u8) -> Option<Vec<u8>> {
            let k = codec.profile.k;
            let per_group = codec.data_per_group;
            let group_data = |group: u8| group * per_group..(group + 1) * per_group;
            if slot < k {
                let group = slot / per_group;
                let mut sources =
                    group_data(group).filter(|member| *member != slot).collect::<Vec<_>>();
                sources.push(k + group);
                Some(sources)
            } else if slot < k + codec.local_groups {
                Some(group_data(slot - k).collect())
            } else {
                None
            }
        }

        /// Deterministic xorshift. Random payloads find what hand-picked ones
        /// miss, and a seed instead of a source of entropy keeps a failure
        /// reproducible.
        struct Rng(u64);

        impl Rng {
            fn next(&mut self) -> u64 {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                self.0
            }

            fn byte(&mut self) -> u8 {
                (self.next() >> 33) as u8
            }

            fn below(&mut self, limit: usize) -> usize {
                let limit = u64::try_from(limit).expect("a slot count fits in u64");
                usize::try_from(self.next() % limit).expect("a value below the limit fits")
            }
        }

        fn standard_lrc_profiles() -> [EcProfileSnapshot; 3] {
            [
                EcProfileSnapshot::lrc_small(),
                EcProfileSnapshot::lrc_standard(),
                EcProfileSnapshot::lrc_dense(),
            ]
        }

        /// A payload of `bytes_per_shard` bytes per data shard, unremarkable
        /// but never a run of zeros: constant input would hide a coefficient
        /// that is wrong and symmetric.
        fn payload_for(profile: &EcProfileSnapshot, bytes_per_shard: u32) -> Vec<u8> {
            (0..bytes_per_shard * u32::from(profile.k))
                .map(|idx| ((idx * 37 + 11) % 251) as u8)
                .collect()
        }

        /// `EC2-LRC-04-001`. The generator itself, before anything is built on
        /// it: a sweep that quietly skipped patterns would prove nothing while
        /// looking exhaustive. Every subset of the slots appears exactly once,
        /// the mask and the flags say the same thing, and the patterns spread
        /// over the number of losses as the binomial coefficients — which is
        /// the shape the fingerprints of EC2-102 are recorded in.
        #[test]
        fn the_generator_walks_every_subset_of_slots_exactly_once() {
            for slots in 1..=14u8 {
                let width = usize::from(slots);
                let mut seen = vec![false; 1 << width];
                let mut spread = vec![0usize; width + 1];

                for pattern in every_loss_pattern(slots) {
                    assert_eq!(pattern.lost.len(), width);
                    for (slot, lost) in pattern.lost.iter().enumerate() {
                        assert_eq!(
                            *lost,
                            pattern.mask & (1 << slot) != 0,
                            "mask {:#b}",
                            pattern.mask
                        );
                    }
                    let missing = pattern.missing_slots();
                    assert!(missing.windows(2).all(|pair| pair[0] < pair[1]));
                    assert_eq!(missing.len(), pattern.lost_count());

                    let index = usize::try_from(pattern.mask).unwrap();
                    assert!(!seen[index], "mask {:#b} appeared twice", pattern.mask);
                    seen[index] = true;
                    spread[pattern.lost_count()] += 1;
                }

                assert!(
                    seen.into_iter().all(|seen| seen),
                    "{slots} slots: a subset was skipped"
                );
                for (lost, count) in spread.iter().enumerate() {
                    assert_eq!(*count, binomial(width, lost), "{slots} slots, {lost} lost");
                }
            }
        }

        /// The matrix the oracle reads has to be the code the encoder writes,
        /// or every comparison below measures the wrong thing. Three claims
        /// together: the encoder is linear, so reading its matrix off basis
        /// vectors is legitimate; the matrix so read reproduces real shards of
        /// a real payload byte for byte; and it is the matrix EC2-100 recorded,
        /// down to the coefficients.
        #[test]
        fn the_matrix_read_off_the_encoder_is_the_one_ec2_100_recorded() {
            // The parity rows of EC2-100, section "Матрицы штатных профилей".
            // Data rows are the identity and are checked as such below.
            let recorded: &[(&str, &[&[u8]])] = &[
                (
                    "ec-lrc-small",
                    &[&[1, 1, 0, 0], &[0, 0, 1, 1], &[82, 247, 2, 166]],
                ),
                (
                    "ec-lrc-standard",
                    &[
                        &[1, 1, 1, 0, 0, 0],
                        &[0, 0, 0, 1, 1, 1],
                        &[186, 105, 211, 210, 104, 187],
                        &[254, 96, 137, 96, 247, 129],
                    ],
                ),
                (
                    "ec-lrc-dense",
                    &[
                        &[1, 1, 1, 1, 1, 0, 0, 0, 0, 0],
                        &[0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
                        &[154, 41, 179, 8, 10, 12, 14, 180, 44, 153],
                        &[34, 75, 104, 187, 156, 254, 216, 10, 14, 5],
                    ],
                ),
            ];

            for (profile, (name, parity_rows)) in standard_lrc_profiles().into_iter().zip(recorded)
            {
                assert_eq!(&profile.name, name);
                let k = usize::from(profile.k);
                let codec = LrcCodec::new(profile.clone()).unwrap();
                let matrix = matrix_from_encoder(&codec);
                assert_eq!(matrix.len(), usize::from(profile.total_shards()));

                // A data slot stores its data shard unchanged, so its row is
                // the unit vector of that column.
                for (slot, row) in matrix.iter().take(k).enumerate() {
                    let unit = (0..k).map(|column| u8::from(column == slot)).collect::<Vec<_>>();
                    assert_eq!(*row, unit, "{name} data slot {slot}");
                }
                assert_eq!(&matrix[k..], *parity_rows, "{name} parity rows");

                // Linearity, and with it the right to read the matrix off
                // basis vectors: every row must hold on a payload of many
                // bytes, not just on the one-byte payloads it was read from.
                let input = payload_for(&profile, 8);
                let data = split_data_shards(&input, profile.k).unwrap();
                let encoded = codec.encode_chunk(&input, 0).unwrap();
                for shard in &encoded.shards {
                    assert_eq!(
                        evaluate(&matrix[usize::from(shard.slot_index)], &data),
                        shard.data.to_vec(),
                        "{name} slot {}",
                        shard.slot_index
                    );
                }
            }
        }

        /// What EC2-102 wrote down for a profile before this code existed.
        struct Expected {
            /// Patterns no decoder recovers, with their spread over the number
            /// of shards lost and the sum of their masks. The sum is what
            /// tells "the right number of patterns" from "the right patterns".
            unrecoverable: usize,
            by_size: &'static [usize],
            mask_sum: u64,
            /// Matrix inversions the oracle needs for the whole sweep, and for
            /// its worst single pattern — the cost EC2-102 measured when it
            /// ruled that the dense profile needs no separate run.
            inversions: usize,
            worst_pattern: usize,
        }

        /// `EC2-LRC-04-002` and `EC2-LRC-04-003`. Every loss pattern of every
        /// standard LRC profile — 128, 1 024 and 16 384 of them — decoded for
        /// real and compared with the independent verdict, in both directions.
        ///
        /// Theory says yes and the codec refuses: durability that was claimed
        /// and is not there. Theory says no and the codec succeeds: a false
        /// positive, and the worse of the two, because the bytes it returns
        /// cannot be right. Either way the sweep fails.
        ///
        /// Every success is also compared with the original payload rather
        /// than with a shard checksum, which on recovered bytes agrees with
        /// whatever it was given, and every refusal has to read as lost data
        /// rather than as a broken request.
        #[test]
        fn every_loss_pattern_decodes_exactly_when_theory_says_it_can() {
            for (profile, expected) in standard_lrc_profiles().into_iter().zip([
                Expected {
                    unrecoverable: 72,
                    by_size: &[0, 0, 0, 8, 35, 21, 7, 1],
                    mask_sum: 5_907,
                    inversions: 75,
                    worst_pattern: 3,
                },
                Expected {
                    unrecoverable: 669,
                    by_size: &[0, 0, 0, 0, 31, 252, 210, 120, 45, 10, 1],
                    mask_sum: 408_801,
                    inversions: 463,
                    worst_pattern: 4,
                },
                Expected {
                    unrecoverable: 15_056,
                    by_size: &[
                        0, 0, 0, 0, 143, 2002, 3003, 3432, 3003, 2002, 1001, 364, 91, 14, 1,
                    ],
                    mask_sum: 129_023_993,
                    inversions: 1_711,
                    worst_pattern: 4,
                },
            ]) {
                let name = profile.name.clone();
                let slots = usize::from(profile.total_shards());
                let codec = LrcCodec::new(profile.clone()).unwrap();
                let matrix = matrix_from_encoder(&codec);
                // Four bytes per shard: what is under test is which patterns
                // recover, not arithmetic on long buffers, and `lrc_dense`
                // alone has 16 384 of them.
                let input = payload_for(&profile, 4);
                let encoded = codec.encode_chunk(&input, 31).unwrap();

                let mut refused = Vec::new();
                let mut spread = vec![0usize; slots + 1];
                let mut inversions = 0;
                let mut worst = 0;

                for pattern in every_loss_pattern(profile.total_shards()) {
                    let verdict = oracle(&matrix, usize::from(profile.k), &pattern.lost);
                    inversions += verdict.inversions;
                    worst = worst.max(verdict.inversions);

                    let shards = options_without(&encoded, &pattern.missing_slots());
                    match codec.decode_chunk(&shards, encoded.original_size) {
                        Ok(decoded) => {
                            assert!(
                                verdict.recoverable,
                                "{name} mask {:#b}: decoded data that nothing determines",
                                pattern.mask
                            );
                            assert_eq!(
                                decoded.as_ref(),
                                input.as_slice(),
                                "{name} mask {:#b}",
                                pattern.mask
                            );
                        }
                        Err(error) => {
                            assert!(
                                !verdict.recoverable,
                                "{name} mask {:#b}: the data is determined and was refused: {error}",
                                pattern.mask
                            );
                            assert!(
                                matches!(error, CodecError::NotRecoverable { .. }),
                                "{name} mask {:#b}: lost data must read as not recoverable, got {error}",
                                pattern.mask
                            );
                            refused.push(pattern.mask);
                            spread[pattern.lost_count()] += 1;
                        }
                    }
                }

                assert_eq!(
                    refused.len(),
                    expected.unrecoverable,
                    "{name} unrecoverable count"
                );
                assert_eq!(spread, expected.by_size, "{name} unrecoverable by size");
                assert_eq!(
                    refused.iter().map(|mask| u64::from(*mask)).sum::<u64>(),
                    expected.mask_sum,
                    "{name} unrecoverable mask sum"
                );
                assert_eq!(inversions, expected.inversions, "{name} oracle inversions");
                assert_eq!(worst, expected.worst_pattern, "{name} worst pattern");
            }
        }

        /// `EC2-LRC-04-002` and `EC2-LRC-04-003` for the repair path: every
        /// loss pattern of every standard profile and every slot it took away —
        /// 448, 5 120 and 114 688 pairs.
        ///
        /// A repair is predicted from two independent facts: the slot's local
        /// group survives intact, or the chunk itself is determined. A shard
        /// that comes back has to be the encoder's own shard whole — the bytes,
        /// and with them the CRC32, the SHA-256, the role and the chunk index,
        /// because a shard stored with a freshly computed checksum over wrong
        /// bytes is exactly the failure this phase exists to rule out.
        #[test]
        fn every_repair_rebuilds_the_encoders_shard_exactly_when_theory_says_it_can() {
            for profile in standard_lrc_profiles() {
                let name = profile.name.clone();
                let codec = LrcCodec::new(profile.clone()).unwrap();
                let matrix = matrix_from_encoder(&codec);
                let input = payload_for(&profile, 4);
                let encoded = codec.encode_chunk(&input, 32).unwrap();
                let mut rebuilt = 0usize;
                let mut refused = 0usize;

                for pattern in every_loss_pattern(profile.total_shards()) {
                    let verdict = oracle(&matrix, usize::from(profile.k), &pattern.lost);
                    let missing = pattern.missing_slots();
                    let shards = options_without(&encoded, &missing);

                    for slot in &missing {
                        let local = group_sources(&codec, *slot).is_some_and(|sources| {
                            sources.iter().all(|source| !pattern.lost[usize::from(*source)])
                        });
                        let repaired = codec.repair_shard(&shards, *slot);
                        assert_eq!(
                            repaired.is_ok(),
                            local || verdict.recoverable,
                            "{name} mask {:#b} slot {slot}",
                            pattern.mask
                        );
                        match repaired {
                            Ok(shard) => {
                                assert_eq!(
                                    shard,
                                    encoded.shards[usize::from(*slot)],
                                    "{name} mask {:#b} slot {slot}",
                                    pattern.mask
                                );
                                rebuilt += 1;
                            }
                            Err(error) => {
                                assert!(
                                    matches!(error, CodecError::NotRecoverable { .. }),
                                    "{name} mask {:#b} slot {slot}: got {error}",
                                    pattern.mask
                                );
                                refused += 1;
                            }
                        }
                    }
                }

                // Every pattern contributes as many pairs as it lost shards,
                // so the sweep owes `n * 2^(n-1)` of them — 448, 5 120 and
                // 114 688. Held to its own size as well as to its results,
                // and to having reached both answers rather than one.
                let slots = usize::from(profile.total_shards());
                assert_eq!(
                    rebuilt + refused,
                    slots << (slots - 1),
                    "{name} pairs visited"
                );
                assert!(
                    rebuilt > 0 && refused > 0,
                    "{name} only ever gave one answer"
                );
            }
        }

        /// The control on the method itself — the last row of EC2-102's table
        /// of negative controls, and the reason every success above is compared
        /// with the original payload.
        ///
        /// A shard carries a CRC32 and a SHA-256 of the bytes it was handed, so
        /// wrong bytes that are internally consistent pass validation and reach
        /// the caller untouched. A sweep that compared checksums instead of
        /// payloads would therefore be unable to fail, which is what this test
        /// demonstrates rather than assumes.
        #[test]
        fn a_shards_own_checksum_cannot_tell_recovered_data_from_rubbish() {
            let codec = LrcCodec::new(EcProfileSnapshot::lrc_standard()).unwrap();
            let input = payload_for(&EcProfileSnapshot::lrc_standard(), 8);
            let encoded = codec.encode_chunk(&input, 41).unwrap();

            let original = &encoded.shards[0];
            let mut rubbish = original.data.to_vec();
            rubbish[0] ^= 0xff;
            let forged = Shard::new(
                original.slot_index,
                original.chunk_index,
                original.role,
                Bytes::from(rubbish),
            )
            .unwrap();

            // Consistent with itself, and that is the whole problem: its own
            // checksums describe the bytes it holds, not the bytes it should.
            forged.verify_integrity().unwrap();
            assert_ne!(forged.sha256, original.sha256);
            assert_ne!(forged.crc32, original.crc32);

            let mut shards = options_without(&encoded, &[]);
            shards[0] = Some(forged);
            let decoded = codec.decode_chunk(&shards, encoded.original_size).unwrap();
            assert_ne!(
                decoded.as_ref(),
                input.as_slice(),
                "the decoder passed the forged bytes through, so only a comparison \
                 against the payload can catch them"
            );
        }

        /// `EC2-LRC-04-004`. The sweeps above run on one payload of four bytes
        /// per shard, which is enough to separate the patterns but says little
        /// about the arithmetic. This draws payloads and patterns at random
        /// instead: 400 draws per profile, half of them losing at most four
        /// shards, where the patterns the solver had to be written for live,
        /// and half losing any number at all, so refusals are drawn too.
        ///
        /// Every draw is checked against the oracle and, when it decodes,
        /// against its own payload. The counts at the end are there so that a
        /// change which quietly stops reaching the solver — or stops reaching a
        /// refusal — shows up as a failure instead of as a still-green test.
        #[test]
        fn random_payloads_decode_from_random_recoverable_patterns() {
            const DRAWS: usize = 400;

            // Floors rather than exact counts: the seed is fixed, so the
            // draws are reproducible, but what matters is that both branches
            // keep being reached, not the precise tally. Measured on this seed:
            // 12, 19 and 37 solver draws out of 400.
            for (profile, least_solver_draws) in
                standard_lrc_profiles().into_iter().zip([10, 15, 30])
            {
                let name = profile.name.clone();
                let slots = usize::from(profile.total_shards());
                let codec = LrcCodec::new(profile.clone()).unwrap();
                let matrix = matrix_from_encoder(&codec);
                let mut rng = Rng(0x5eed_0000_0006_0400 ^ u64::from(profile.k));
                let mut decoded_count = 0usize;
                let mut refused_count = 0usize;
                let mut solver_draws = 0usize;

                for draw in 0..DRAWS {
                    let size = rng.below(8 * usize::from(profile.k) + 1);
                    let input = (0..size).map(|_| rng.byte()).collect::<Vec<_>>();
                    let encoded = codec.encode_chunk(&input, 33).unwrap();

                    // A random subset of the given size: shuffle the slots and
                    // take a prefix, so no slot is ever drawn twice.
                    let wanted = rng.below(if draw % 2 == 0 { 5 } else { slots + 1 });
                    let mut order = (0..slots).collect::<Vec<_>>();
                    for position in 0..wanted {
                        order.swap(position, position + rng.below(slots - position));
                    }
                    let mut lost = vec![false; slots];
                    for slot in order.iter().take(wanted) {
                        lost[*slot] = true;
                    }
                    let pattern = LossPattern::new(lost);

                    let verdict = oracle(&matrix, usize::from(profile.k), &pattern.lost);
                    let fast = LrcCodec::fast_path_after_loss(
                        profile.k,
                        codec.local_groups,
                        codec.global_parity,
                        &pattern.lost,
                    );
                    if verdict.recoverable && !fast.recovers() {
                        solver_draws += 1;
                    }

                    let missing = pattern.missing_slots();
                    let shards = options_without(&encoded, &missing);
                    match codec.decode_chunk(&shards, encoded.original_size) {
                        Ok(decoded) => {
                            assert!(
                                verdict.recoverable,
                                "{name} draw {draw}, {size} byte(s), lost {missing:?}: \
                                 decoded data that nothing determines"
                            );
                            assert_eq!(
                                decoded.as_ref(),
                                input.as_slice(),
                                "{name} draw {draw}, {size} byte(s), lost {missing:?}"
                            );
                            decoded_count += 1;
                        }
                        Err(error) => {
                            assert!(
                                !verdict.recoverable,
                                "{name} draw {draw}, {size} byte(s), lost {missing:?}: \
                                 the data is determined and was refused: {error}"
                            );
                            refused_count += 1;
                        }
                    }
                }

                assert_eq!(decoded_count + refused_count, DRAWS, "{name}");
                assert!(
                    refused_count > 0,
                    "{name} never drew a pattern beyond the code"
                );
                assert!(
                    solver_draws >= least_solver_draws,
                    "{name}: only {solver_draws} draw(s) reached the solver"
                );
            }
        }

        /// `EC2-LRC-04-004`, at the input sizes of EC2-002: an empty payload,
        /// one byte, fewer bytes than data shards, an exact multiple of `k`,
        /// and an odd size that is a multiple of nothing. Each one is encoded,
        /// loses a pattern only the unified solver reaches, and has to come
        /// back byte for byte — and the slot it lost has to repair into the
        /// encoder's own shard.
        ///
        /// A full chunk, the size a real object arrives in, runs on
        /// `lrc_small` only: 6 MiB through a debug build is by far the most
        /// expensive case here, and the solver's work per byte position does
        /// not depend on the profile. Taking it one byte short of the chunk
        /// makes it the non-multiple case at the same time.
        #[test]
        fn boundary_input_sizes_decode_through_the_solver() {
            for profile in standard_lrc_profiles() {
                let name = profile.name.clone();
                let k = usize::from(profile.k);
                let codec = LrcCodec::new(profile.clone()).unwrap();
                let mut sizes = vec![0, 1, k - 1, k, 3 * k, 65_537];
                if profile.name == "ec-lrc-small" {
                    sizes.push(usize::try_from(profile.chunk_size).unwrap() - 1);
                }

                // The first `g + 1` data shards of group 0: more than global
                // parity alone carries, so the fast paths refuse the pattern
                // and the solver answers through the group's local parity.
                let missing = (0..=codec.global_parity).collect::<Vec<u8>>();
                let lost = (0..profile.total_shards())
                    .map(|slot| missing.contains(&slot))
                    .collect::<Vec<_>>();
                assert!(
                    !LrcCodec::fast_path_after_loss(
                        profile.k,
                        codec.local_groups,
                        codec.global_parity,
                        &lost
                    )
                    .recovers(),
                    "{name}: the pattern no longer needs the solver"
                );

                let mut rng = Rng(0xda7a_0000_0006_0400);
                for size in sizes {
                    let input = (0..size).map(|_| rng.byte()).collect::<Vec<_>>();
                    let encoded = codec.encode_chunk(&input, 34).unwrap();
                    let shards = options_without(&encoded, &missing);

                    let decoded = codec.decode_chunk(&shards, encoded.original_size).unwrap();
                    assert_eq!(decoded.as_ref(), input.as_slice(), "{name}, {size} byte(s)");
                    assert_eq!(
                        codec.repair_shard(&shards, missing[0]).unwrap(),
                        encoded.shards[usize::from(missing[0])],
                        "{name}, {size} byte(s)"
                    );
                }
            }
        }
    }
}
