// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Which shard losses a profile survives, answered without any shards.
//!
//! Placement needs to know whether a chunk is still decodable after a whole
//! zone or rack disappears, long before any data is written. Counting labels
//! cannot answer that for LRC: whether a loss is survivable depends on how it
//! falls across the local groups and on what the decoder does with that. The
//! answer therefore comes from the codec's own criterion — for LRC, the rank
//! of the unified matrix the decoder solves by — and a test holds the two
//! equal on every loss pattern of every standard profile (decision EC2-082,
//! "Decoder predicate").

use crate::rs::ReedSolomonCodec;
use crate::types::{CodecEngine, CodecError, EcParity, EcProfileSnapshot, RecoveryPath};
use crate::LrcCodec;

/// The loss patterns a profile's decoder rebuilds a chunk from.
///
/// Built once from a profile and then asked many times, which is how
/// placement uses it: one question per zone or rack for every layout it
/// considers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LossTolerance {
    k: u8,
    parity: EcParity,
    decoder: Decoder,
}

/// What the profile's decoder rests on, prepared once so that every later
/// question is just arithmetic.
///
/// Modelled as one or the other rather than as an optional field, so there is
/// no shape in which an LRC profile could be asked without its matrix.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Decoder {
    /// Any `k` of `k + m` shards decode, so only the number of losses matters.
    ReedSolomon(ReedSolomonCodec),
    /// The LRC decoder, where the pattern matters as much as the count.
    Lrc(LrcCodec),
}

impl Decoder {
    /// The engine itself, so that every question below is put to the decoder
    /// that will actually run rather than to a second description of it.
    fn engine(&self) -> &dyn CodecEngine {
        match self {
            Self::ReedSolomon(codec) => codec,
            Self::Lrc(codec) => codec,
        }
    }
}

impl LossTolerance {
    /// The tolerance of `profile`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::InvalidProfile`] when the profile is not valid.
    pub fn new(profile: &EcProfileSnapshot) -> Result<Self, CodecError> {
        profile.validate()?;
        let decoder = match profile.parity {
            EcParity::ReedSolomon { .. } => {
                Decoder::ReedSolomon(ReedSolomonCodec::new(profile.clone())?)
            }
            EcParity::Lrc { .. } => Decoder::Lrc(LrcCodec::new(profile.clone())?),
        };
        Ok(Self {
            k: profile.k,
            parity: profile.parity,
            decoder,
        })
    }

    /// Number of shard slots the profile has.
    pub fn total_shards(&self) -> usize {
        usize::from(self.k) + usize::from(self.parity.total_parity())
    }

    /// The most shards a chunk can lose, whichever they are, and still decode.
    ///
    /// For Reed-Solomon that is `m`. For LRC the count alone does not decide,
    /// so every pattern of each count is put to the decoder, from one loss up,
    /// and the answer is the count before the first pattern it cannot rebuild.
    /// This is the number of node failures an erasure set of the profile is
    /// guaranteed to survive, one shard per node.
    pub fn guaranteed_losses(&self) -> u8 {
        let total = self.total_shards();
        let mut guaranteed = 0u8;
        for count in 1..=total {
            if !every_pattern_of(total, count, |lost| self.can_decode(lost)) {
                break;
            }
            guaranteed = u8::try_from(count).unwrap_or(u8::MAX);
        }
        guaranteed
    }

    /// Whether `decode_chunk` rebuilds a chunk after losing the slots marked
    /// in `lost`.
    ///
    /// `lost[slot]` is true when the shard of that slot is gone. A slot past
    /// the end of `lost` counts as present.
    ///
    /// For LRC this is the full decoder, fast paths and unified solver alike:
    /// the fast paths reach a subset of what the solver reaches, so its rank
    /// criterion answers for both. Which of them does the work, and therefore
    /// what a degraded read costs, is a separate question — see
    /// [`CodecEngine::failure_matrix`](crate::CodecEngine::failure_matrix).
    pub fn can_decode(&self, lost: &[bool]) -> bool {
        self.recovery_path(lost).recovers()
    }

    /// Which path [`crate::CodecEngine::decode_chunk`] takes to rebuild a chunk
    /// after losing the slots marked in `lost`.
    ///
    /// [`Self::can_decode`] answers whether the data comes back; this answers
    /// what getting it back costs, and names the patterns that hold only
    /// because of the unified solver. The read and repair paths ask it after
    /// the fact, so that a recovery only the solver reached is written down
    /// rather than passed over in silence (decision EC2-103, section 5).
    pub fn recovery_path(&self, lost: &[bool]) -> RecoveryPath {
        self.decoder.engine().recovery_path(lost)
    }

    /// The slots `repair_shard` rebuilds `slot` from without decoding the
    /// chunk, or `None` when the profile has no such path for it.
    ///
    /// Only LRC has one: the other members of the slot's local group, `k /
    /// local` shards, XOR-ed together. A global parity slot and every
    /// Reed-Solomon slot are rebuilt by decoding, from any shards that pass
    /// [`Self::can_decode`].
    pub fn local_repair_sources(&self, slot: u8) -> Option<Vec<u8>> {
        match self.parity {
            EcParity::ReedSolomon { .. } => None,
            EcParity::Lrc { local, .. } => LrcCodec::local_repair_sources(self.k, local, slot),
        }
    }

    /// Whether `repair_shard` rebuilds `slot` once the slots marked in `lost`
    /// are gone.
    ///
    /// `slot` itself counts as lost whatever `lost` says, and a slot past the
    /// end of `lost` counts as present. True when every source of the local
    /// path survives, or when the chunk still decodes.
    pub fn can_repair(&self, slot: u8, lost: &[bool]) -> bool {
        self.repair_path(slot, lost).recovers()
    }

    /// Which path [`crate::CodecEngine::repair_shard`] takes to rebuild `slot`
    /// once the slots marked in `lost` are gone.
    ///
    /// `slot` itself counts as lost whatever `lost` says, and a slot past the
    /// end of `lost` counts as present.
    ///
    /// The path names what the repair costs and which edition of the decoder
    /// can do it, not which function runs. [`RecoveryPath::LocalXor`] is the
    /// local path: `k / local` reads inside the group and nothing else. Every
    /// other repair rebuilds the slot as one combination of `k` shards, which
    /// costs what the global stage costs and is available exactly when the
    /// chunk is determined — so it is named [`RecoveryPath::GlobalRs`], except
    /// where the chunk is determined only through the unified solver. Those
    /// the repair worker logs and counts: they are the shards that would not
    /// have been rebuilt at all before the solver existed (decision EC2-103,
    /// section 5).
    pub fn repair_path(&self, slot: u8, lost: &[bool]) -> RecoveryPath {
        let total = self.total_shards();
        if usize::from(slot) >= total {
            return RecoveryPath::None;
        }
        let mut lost = lost.to_vec();
        lost.resize(total.max(lost.len()), false);
        lost[usize::from(slot)] = true;

        let local = self
            .local_repair_sources(slot)
            .is_some_and(|sources| sources.iter().all(|source| !lost[usize::from(*source)]));
        if local {
            return RecoveryPath::LocalXor;
        }
        match self.recovery_path(&lost) {
            RecoveryPath::None => RecoveryPath::None,
            RecoveryPath::UnifiedSolver => RecoveryPath::UnifiedSolver,
            _ => RecoveryPath::GlobalRs,
        }
    }
}

/// Whether `check` holds for every way of losing `count` of `total` slots.
fn every_pattern_of(total: usize, count: usize, mut check: impl FnMut(&[bool]) -> bool) -> bool {
    // The lost slots, in increasing order, stepped through every combination.
    let mut chosen: Vec<usize> = (0..count).collect();
    let mut lost = vec![false; total];
    loop {
        lost.iter_mut().for_each(|slot| *slot = false);
        for &slot in &chosen {
            lost[slot] = true;
        }
        if !check(&lost) {
            return false;
        }
        // The rightmost slot that can still move right, and everything after
        // it packed behind it.
        let Some(at) = (0..count).rev().find(|&i| chosen[i] < total - count + i) else {
            return true;
        };
        chosen[at] += 1;
        for i in at + 1..count {
            chosen[i] = chosen[i - 1] + 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{build_codec, Shard};

    /// Reed-Solomon survives `m` losses; LRC survives one more than its
    /// global parities, which every pattern of each count proves.
    #[test]
    fn the_guaranteed_losses_of_every_standard_profile() {
        let guaranteed: Vec<(String, u8)> = standard_profiles()
            .iter()
            .map(|profile| {
                (
                    profile.name.clone(),
                    LossTolerance::new(profile).unwrap().guaranteed_losses(),
                )
            })
            .collect();
        assert_eq!(
            guaranteed,
            [
                ("ec-rs-small".to_string(), 2),
                ("ec-rs-standard".to_string(), 3),
                ("ec-rs-dense".to_string(), 3),
                ("ec-lrc-small".to_string(), 2),
                ("ec-lrc-standard".to_string(), 3),
                ("ec-lrc-dense".to_string(), 3),
            ]
        );
    }

    #[test]
    fn every_pattern_is_tried_once() {
        let mut seen = Vec::new();
        assert!(every_pattern_of(5, 2, |lost| {
            seen.push(lost.to_vec());
            true
        }));
        assert_eq!(seen.len(), 10);
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 10);
        assert!(seen.iter().all(|lost| lost.iter().filter(|l| **l).count() == 2));
        assert!(!every_pattern_of(4, 1, |lost| !lost[3]));
    }

    fn standard_profiles() -> [EcProfileSnapshot; 6] {
        [
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ]
    }

    /// What a degraded read costs, held against what it returns.
    ///
    /// The verdict and the path are two answers to one question and must not
    /// drift apart: a path that recovers exactly where the chunk decodes,
    /// `Direct` exactly where no data shard is missing (nothing to rebuild, so
    /// nothing to pay for), and `UnifiedSolver` only where a real decode
    /// really does fall through to the solver — which the decoder's own
    /// counter, not this module, is what proves.
    #[test]
    fn the_path_named_for_a_loss_agrees_with_the_verdict() {
        for profile in standard_profiles() {
            let tolerance = LossTolerance::new(&profile).unwrap();
            let n = tolerance.total_shards();

            for mask in 0u32..(1 << n) {
                let lost = (0..n).map(|slot| mask & (1 << slot) != 0).collect::<Vec<_>>();
                let path = tolerance.recovery_path(&lost);
                let data_intact = (0..usize::from(profile.k)).all(|slot| !lost[slot]);

                assert_eq!(
                    path.recovers(),
                    tolerance.can_decode(&lost),
                    "{} mask {mask:#b}",
                    profile.name
                );
                assert_eq!(
                    path == RecoveryPath::Direct,
                    data_intact,
                    "{} mask {mask:#b}: only an intact data set is free to read",
                    profile.name
                );
                if profile.codec != crate::types::EcCodecKind::LocallyRepairable {
                    assert_ne!(
                        path,
                        RecoveryPath::LocalXor,
                        "{} has no local groups",
                        profile.name
                    );
                    assert_ne!(
                        path,
                        RecoveryPath::UnifiedSolver,
                        "{} has nothing to unify",
                        profile.name
                    );
                }
            }
        }
    }

    /// Every loss pattern of every standard profile: the predicate answers
    /// exactly what the real decoder does — up to 2^14 = 16 384 patterns per
    /// profile. If the decoder changes, this fails until the predicate
    /// follows, so placement never plans around a decoder that no longer
    /// exists. Plan 06 did change it, and the counts below moved with it.
    #[test]
    fn the_predicate_matches_decode_chunk_on_every_loss_pattern() {
        for profile in standard_profiles() {
            let codec = build_codec(profile.clone()).unwrap();
            let tolerance = LossTolerance::new(&profile).unwrap();
            let n = tolerance.total_shards();
            assert_eq!(n, usize::from(profile.total_shards()));
            let input = (0..64 * u32::from(profile.k)).map(|i| (i % 251) as u8).collect::<Vec<_>>();
            let encoded = codec.encode_chunk(&input, 0).unwrap();

            let mut undecodable = 0usize;
            for mask in 0u32..(1 << n) {
                let lost = (0..n).map(|slot| mask & (1 << slot) != 0).collect::<Vec<_>>();
                let shards = encoded
                    .shards
                    .iter()
                    .map(|shard| (!lost[usize::from(shard.slot_index)]).then(|| shard.clone()))
                    .collect::<Vec<Option<Shard>>>();
                let decoded = codec.decode_chunk(&shards, encoded.original_size);
                if let Ok(bytes) = &decoded {
                    assert_eq!(
                        bytes.as_ref(),
                        input.as_slice(),
                        "{} mask {mask:#b}",
                        profile.name
                    );
                }
                assert_eq!(
                    tolerance.can_decode(&lost),
                    decoded.is_ok(),
                    "{} mask {mask:#b}",
                    profile.name
                );
                undecodable += usize::from(decoded.is_err());
            }

            // The counts EC2-102 predicted for the unified decoder, before it
            // was written: 8, 96 and 617 patterns fewer than the two-stage
            // decoder of EC v1 left unrecoverable (80, 765 and 15 673). For
            // RS, every loss of more than `m` shards.
            let expected = match profile.name.as_str() {
                "ec-lrc-small" => 72,
                "ec-lrc-standard" => 669,
                "ec-lrc-dense" => 15_056,
                _ => {
                    let parity = n - usize::from(profile.k);
                    (parity + 1..=n).map(|lost| binomial(n, lost)).sum()
                }
            };
            assert_eq!(undecodable, expected, "{}", profile.name);
        }
    }

    fn binomial(n: usize, k: usize) -> usize {
        (0..k).fold(1, |acc, i| acc * (n - i) / (i + 1))
    }

    /// The facts EC2-080 builds on: a whole local group is never decodable,
    /// while a loss taking at most one member of each group — with any global
    /// parity shards on top — always is.
    #[test]
    fn a_whole_group_is_lost_and_one_member_per_group_is_not() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let EcParity::Lrc { local, global } = profile.parity else {
                unreachable!("an LRC profile");
            };
            let (k, local, global) = (
                usize::from(profile.k),
                usize::from(local),
                usize::from(global),
            );
            let per_group = k / local;
            let tolerance = LossTolerance::new(&profile).unwrap();
            let n = tolerance.total_shards();

            let mut whole_group = vec![false; n];
            whole_group[..per_group].fill(true);
            whole_group[k] = true;
            assert!(!tolerance.can_decode(&whole_group), "{}", profile.name);

            // One member of every group — data or local parity — plus every
            // global parity shard.
            for member in 0..=per_group {
                let mut lost = vec![false; n];
                for group in 0..local {
                    let slot = if member == per_group {
                        k + group
                    } else {
                        group * per_group + member
                    };
                    lost[slot] = true;
                }
                for index in 0..global {
                    lost[k + local + index] = true;
                }
                assert!(
                    tolerance.can_decode(&lost),
                    "{} member {member}",
                    profile.name
                );
            }
        }
    }

    /// Every loss pattern of every standard profile, and every lost slot in
    /// it: `can_repair` answers exactly what `repair_shard` does, and a repair
    /// that succeeds returns the original bytes. Repair planning chooses which
    /// shards to fetch from this answer, so a disagreement would send a repair
    /// out with sources the codec cannot use.
    #[test]
    fn the_repair_predicate_matches_repair_shard_on_every_loss_pattern() {
        for profile in standard_profiles() {
            let codec = build_codec(profile.clone()).unwrap();
            let tolerance = LossTolerance::new(&profile).unwrap();
            let n = tolerance.total_shards();
            // Four-byte shards: the patterns are what is under test, not the
            // arithmetic on long buffers, and there are 114 688 repairs of
            // `lrc_dense` alone.
            let input =
                (0..4 * u32::from(profile.k)).map(|i| (i * 7 % 251) as u8).collect::<Vec<_>>();
            let encoded = codec.encode_chunk(&input, 0).unwrap();

            for mask in 1u32..(1 << n) {
                let lost = (0..n).map(|slot| mask & (1 << slot) != 0).collect::<Vec<_>>();
                let shards = encoded
                    .shards
                    .iter()
                    .map(|shard| (!lost[usize::from(shard.slot_index)]).then(|| shard.clone()))
                    .collect::<Vec<Option<Shard>>>();
                for slot in (0..n).filter(|slot| lost[*slot]) {
                    let slot = u8::try_from(slot).unwrap();
                    let repaired = codec.repair_shard(&shards, slot);
                    assert_eq!(
                        tolerance.can_repair(slot, &lost),
                        repaired.is_ok(),
                        "{} mask {mask:#b} slot {slot}",
                        profile.name
                    );
                    // The path is the same answer told in more detail, so it
                    // has to agree with the verdict everywhere, and name the
                    // local path exactly where the local sources survived —
                    // which is what the repair worker charges its reads to.
                    let path = tolerance.repair_path(slot, &lost);
                    assert_eq!(
                        path.recovers(),
                        repaired.is_ok(),
                        "{} mask {mask:#b} slot {slot}",
                        profile.name
                    );
                    let local_open = tolerance.local_repair_sources(slot).is_some_and(|sources| {
                        sources.iter().all(|source| !lost[usize::from(*source)])
                    });
                    assert_eq!(
                        path == RecoveryPath::LocalXor,
                        local_open,
                        "{} mask {mask:#b} slot {slot}",
                        profile.name
                    );
                    // And `unified_solver` exactly where no EC v1 path would
                    // have rebuilt the shard: the group cannot do it, and the
                    // chunk itself is out of reach of the fast paths. This is
                    // the event the repair worker writes down.
                    let mut lost_with_slot = lost.clone();
                    lost_with_slot[usize::from(slot)] = true;
                    let only_the_solver =
                        tolerance.recovery_path(&lost_with_slot) == RecoveryPath::UnifiedSolver;
                    assert_eq!(
                        path == RecoveryPath::UnifiedSolver,
                        !local_open && only_the_solver,
                        "{} mask {mask:#b} slot {slot}",
                        profile.name
                    );
                    if let Ok(shard) = repaired {
                        assert_eq!(
                            shard.data,
                            encoded.shards[usize::from(slot)].data,
                            "{} mask {mask:#b} slot {slot}",
                            profile.name
                        );
                    }
                }
            }
        }
    }

    /// The local path reads one group less the lost member: `k / local`
    /// shards — 2, 3 and 5 for the three LRC profiles — and a local parity
    /// shard is rebuilt from its group's data alone. Reed-Solomon has no local
    /// path, nor does a global parity slot.
    #[test]
    fn a_local_repair_reads_the_rest_of_the_group_and_nothing_else() {
        for (profile, per_group) in [
            (EcProfileSnapshot::lrc_small(), 2usize),
            (EcProfileSnapshot::lrc_standard(), 3),
            (EcProfileSnapshot::lrc_dense(), 5),
        ] {
            let tolerance = LossTolerance::new(&profile).unwrap();
            let EcParity::Lrc { local, .. } = profile.parity else {
                unreachable!("an LRC profile");
            };
            let k = profile.k;
            let group_of = |slot: u8| {
                if slot < k {
                    slot / per_group as u8
                } else {
                    slot - k
                }
            };

            for slot in 0..profile.total_shards() {
                let sources = tolerance.local_repair_sources(slot);
                if slot >= k + local {
                    assert_eq!(sources, None, "{} global slot {slot}", profile.name);
                    continue;
                }
                let sources = sources.unwrap();
                assert_eq!(sources.len(), per_group, "{} slot {slot}", profile.name);
                assert!(!sources.contains(&slot), "{} slot {slot}", profile.name);
                assert!(
                    sources.iter().all(|source| group_of(*source) == group_of(slot)),
                    "{} slot {slot}: {sources:?}",
                    profile.name
                );
                if slot >= k {
                    assert!(sources.iter().all(|source| *source < k), "{}", profile.name);
                }

                // Everything outside the sources gone, and the repair still
                // succeeds.
                let mut lost = vec![true; tolerance.total_shards()];
                for source in &sources {
                    lost[usize::from(*source)] = false;
                }
                assert!(
                    tolerance.can_repair(slot, &lost),
                    "{} slot {slot}",
                    profile.name
                );
            }
        }
        for profile in [
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
        ] {
            let tolerance = LossTolerance::new(&profile).unwrap();
            assert!((0..profile.total_shards())
                .all(|slot| tolerance.local_repair_sources(slot).is_none()));
        }
    }

    #[test]
    fn slots_past_the_end_count_as_present() {
        let tolerance = LossTolerance::new(&EcProfileSnapshot::lrc_standard()).unwrap();
        assert!(tolerance.can_decode(&[]));
        assert!(tolerance.can_decode(&[true, true]));
        // All three data shards of local group 0, with everything past the end
        // of the pattern alive — the group's local parity among them, and that
        // is what carries this loss (EC2-101). Take the local parity away as
        // well and no decoder recovers it.
        assert!(tolerance.can_decode(&[true, true, true]));
        assert!(!tolerance.can_decode(&[true, true, true, false, false, false, true]));
    }

    #[test]
    fn an_invalid_profile_has_no_tolerance() {
        let mut profile = EcProfileSnapshot::lrc_standard();
        profile.k = 0;
        assert!(LossTolerance::new(&profile).is_err());
    }
}
