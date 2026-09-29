// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Unified local-plus-global solver for the LRC codec.
//!
//! The two-stage decoder in [`crate::lrc`] rebuilds a local group by XOR only
//! when the group misses exactly one data shard, and hands whatever is left to
//! Reed-Solomon, which ignores local parity entirely. A group that lost two
//! data shards falls between the stages: its local equation exists, is
//! available, and goes unused.
//!
//! This module stops dividing one system in two. Every slot of an LRC profile
//! holds a linear combination of the `k` data shards, so the whole code is one
//! matrix of `k + l + g` rows over GF(2^8) — a unit row per data slot, a row of
//! ones over its own group per local parity slot, and a row of the Reed-Solomon
//! generator per global parity slot. The data is determined exactly when the
//! rows of the surviving shards have rank `k`, and that is the criterion the
//! solver answers by. See decision EC2-100.
//!
//! Encoding is untouched. The matrix is derived from the encoder rather than
//! declared next to it, and a test substitutes real encoded shards into every
//! row to keep it that way.

use crate::gf::{self, solver};
use crate::rs::{combine, systematic_generator_matrix, ShardCandidate};
use crate::types::CodecError;

#[cfg(test)]
thread_local! {
    /// Unified solves started on this thread.
    ///
    /// Per thread rather than global because tests run in parallel and a shared
    /// counter would report another test's work. It exists so that the promise
    /// "the fast paths never reach the solver" can be observed instead of
    /// assumed — see `the_fast_paths_never_reach_the_solver` in [`crate::lrc`].
    static SOLVES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The unified matrix of one LRC profile, and the recovery it supports.
///
/// Built once per profile and asked many times: the matrix depends on the
/// profile alone, while the shards change with every chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LrcSolver {
    k: u8,
    /// One row of `k` coefficients per slot, indexed by slot.
    rows: Vec<Vec<u8>>,
}

impl LrcSolver {
    /// Builds the unified matrix of `LRC(k, local, global)`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::InvalidProfile`] when the shape is not an LRC
    /// profile this crate can build, and [`CodecError::Arithmetic`] when the
    /// Reed-Solomon generator matrix cannot be formed.
    pub(crate) fn new(k: u8, local: u8, global: u8) -> Result<Self, CodecError> {
        let invalid = |reason: String| CodecError::InvalidProfile {
            profile: "lrc-solver".to_string(),
            reason,
        };
        if k == 0 || local == 0 || global == 0 {
            return Err(invalid(format!(
                "LRC needs k, local and global above zero, got k={k}, local={local}, global={global}"
            )));
        }
        if !k.is_multiple_of(local) {
            return Err(invalid(format!(
                "k={k} does not split evenly into {local} local group(s)"
            )));
        }
        let total = u16::from(k) + u16::from(local) + u16::from(global);
        if total > u16::from(u8::MAX) {
            return Err(invalid(format!(
                "total shard count must fit in u8, got {total}"
            )));
        }

        // `k + global` cannot overflow: it is no larger than the total checked
        // just above.
        let generator = systematic_generator_matrix(k, k + global)?;
        let data_per_group = k / local;
        let mut rows = Vec::with_capacity(usize::from(k) + usize::from(local + global));

        // Data slots pass through unchanged, so each one reads a single byte of
        // the data.
        for slot in 0..k {
            let mut row = vec![0; usize::from(k)];
            row[usize::from(slot)] = 1;
            rows.push(row);
        }
        // A local parity shard is the XOR of its group. XOR is addition in
        // GF(2^8) and multiplying by one is the identity, so the same row form
        // covers it: ones over the group's columns, zeros elsewhere. Nothing
        // about these rows belongs to "another code".
        for group in 0..local {
            let mut row = vec![0; usize::from(k)];
            for slot in group * data_per_group..(group + 1) * data_per_group {
                row[usize::from(slot)] = 1;
            }
            rows.push(row);
        }
        // Global parity slot `j` stores row `k + j` of the (k, k + global)
        // Reed-Solomon code — the very matrix `LrcCodec` encodes it with.
        rows.extend(generator.into_iter().skip(usize::from(k)));

        Ok(Self { k, rows })
    }

    /// The matrix row of one slot: the coefficients whose combination of the
    /// data shards is the shard stored there.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::InvalidShardSet`] when the slot is outside the
    /// profile.
    pub(crate) fn row(&self, slot: u8) -> Result<&[u8], CodecError> {
        self.rows.get(usize::from(slot)).map(Vec::as_slice).ok_or_else(|| {
            CodecError::InvalidShardSet {
                reason: format!(
                    "slot {slot} is outside an LRC profile with {} slot(s)",
                    self.rows.len()
                ),
            }
        })
    }

    /// Rebuilds all `k` data shards from the shards that are still present.
    ///
    /// Succeeds exactly when the rows of the present shards have rank `k`,
    /// which is the point at which the data stops being ambiguous. The answer
    /// is either the data the encoder started from or an error: `k` independent
    /// rows are picked and inverted, so a pattern short of rank has no square
    /// block to invert and is refused rather than completed with a guess. A
    /// guess would be a plausible-looking answer that is almost certainly
    /// wrong, and no checksum below this point would catch it.
    ///
    /// The system solved is the one the given shards describe. That their bytes
    /// really are the bytes that were stored is [`crate::rs::ValidShardSet`]'s
    /// job, which the Reed-Solomon decoder relies on in the same way.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::NotRecoverable`] when too much is gone, and
    /// [`CodecError::InvalidShardSet`] when the shard set itself is unusable —
    /// an unknown slot, the same slot twice, or shards of different sizes.
    pub(crate) fn recover_data_shards(
        &self,
        present: &[ShardCandidate],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let selection = self.select(present)?;
        let inverse = gf::invert_matrix(&selection.block)?;
        Ok(inverse.iter().map(|row| combine(row, &selection.shards)).collect())
    }

    /// Rebuilds the shard of one slot without going through the whole chunk.
    ///
    /// Repair asks for a single slot, and reaching it through every data shard
    /// costs `k` combinations and a re-encode where one combination does. The
    /// coefficients that express slot `slot` in terms of the shards on hand
    /// solve `Aᵀ c = M[slot]` once per chunk, independently of shard size, and
    /// the shard is then a single combination of `k` shards. On `lrc_dense`
    /// that is ten times less byte work than decoding and encoding again, on a
    /// path that runs continuously in the background.
    ///
    /// Answers on exactly the condition the whole chunk does — rank `k`. A slot
    /// that is cheaper than that, a group member whose fellow members all
    /// survive, belongs to the XOR fast path, and [`crate::LrcCodec`] tries that
    /// before it reaches the solver. Keeping the two apart is what makes the
    /// repair reach recorded in EC2-101 hold: the local path, or the chunk
    /// decodes.
    ///
    /// The slot need not be missing. The solver evaluates its row, which for a
    /// slot still present reproduces the shard already held — useful for
    /// checking one shard against the others, and never a way to overwrite it,
    /// because `repair_shard` establishes that the slot is absent first.
    ///
    /// # Errors
    ///
    /// As [`Self::recover_data_shards`], plus [`CodecError::InvalidShardSet`]
    /// when `slot` is outside the profile.
    pub(crate) fn recover_slot(
        &self,
        present: &[ShardCandidate],
        slot: u8,
    ) -> Result<Vec<u8>, CodecError> {
        let target = self.row(slot)?.to_vec();
        let selection = self.select(present)?;
        // The block is square with independent rows, so its transpose is too:
        // these coefficients always exist, and they are the only ones.
        let coefficients = solver::solve(&transpose(&selection.block), &target)?;
        Ok(combine(&coefficients, &selection.shards))
    }

    /// Whether [`Self::recover_data_shards`] rebuilds the data once the slots
    /// marked in `lost` are gone, answered without any shards.
    ///
    /// `lost[slot]` is true when that shard is gone, and a slot past the end of
    /// `lost` counts as present — the convention [`crate::LossTolerance`] uses,
    /// which is the form placement asks in. The answer is the rank of the
    /// surviving rows, which is the same question the recovery itself puts, so
    /// the two cannot drift apart.
    pub(crate) fn recovers_after_loss(&self, lost: &[bool]) -> bool {
        let available = self
            .rows
            .iter()
            .enumerate()
            .filter(|(slot, _)| !lost.get(*slot).copied().unwrap_or(false))
            .map(|(_, row)| row.clone())
            .collect::<Vec<_>>();
        if available.len() < usize::from(self.k) {
            return false;
        }
        solver::rank(&available).is_ok_and(|rank| rank == usize::from(self.k))
    }

    /// The `k` independent equations a recovery runs through, and the shards
    /// that supply them.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::NotRecoverable`] when the surviving rows fall short
    /// of rank `k`, and [`CodecError::InvalidShardSet`] when the shard set is
    /// unusable.
    fn select(&self, present: &[ShardCandidate]) -> Result<Selection, CodecError> {
        // Counted here rather than at the two entry points: this is the one
        // place a recovery turns into a solve, refusals included.
        #[cfg(test)]
        SOLVES.with(|solves| solves.set(solves.get() + 1));

        let equations = self.equations(present)?;
        let unknowns = usize::from(self.k);
        let chosen = solver::independent_rows(&equations, unknowns)?;
        let columns = (0..unknowns).collect::<Vec<_>>();
        let block = solver::submatrix(&equations, &chosen, &columns)?;

        // Only the chosen shards are copied. The others took part in the rank
        // question and have nothing left to contribute.
        let shards = chosen
            .iter()
            .map(|index| {
                present.get(*index).map(|candidate| candidate.data.to_vec()).ok_or_else(|| {
                    CodecError::Arithmetic {
                        reason: format!(
                            "row {index} is outside the {} present shard(s)",
                            present.len()
                        ),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Selection { block, shards })
    }

    /// The equations the present shards contribute, in the order given.
    ///
    /// Row `i` belongs to `present[i]`, so rows and shards stay index-aligned
    /// from the rank question through to the combination that ends it.
    fn equations(&self, present: &[ShardCandidate]) -> Result<Vec<Vec<u8>>, CodecError> {
        let Some(first) = present.first() else {
            return Err(CodecError::NotRecoverable {
                reason: "no shards are present".to_string(),
            });
        };

        // A repeated slot would add an equation the set already holds. The rank
        // would not change, but it means the caller built the set wrong, and a
        // decoder that quietly accepts a malformed set hides that.
        let mut slots = present.iter().map(|candidate| candidate.slot_index).collect::<Vec<_>>();
        slots.sort_unstable();
        slots.dedup();
        if slots.len() != present.len() {
            return Err(CodecError::InvalidShardSet {
                reason: "the same slot appears more than once".to_string(),
            });
        }

        let shard_size = first.data.len();
        present
            .iter()
            .map(|candidate| {
                if candidate.data.len() != shard_size {
                    return Err(CodecError::InvalidShardSet {
                        reason: format!(
                            "mixed shard sizes: {shard_size} and {}",
                            candidate.data.len()
                        ),
                    });
                }
                Ok(self.row(candidate.slot_index)?.to_vec())
            })
            .collect()
    }
}

#[cfg(test)]
impl LrcSolver {
    /// Unified solves this thread has started, refusals included.
    pub(crate) fn solves_on_this_thread() -> usize {
        SOLVES.with(std::cell::Cell::get)
    }
}

/// The `k` independent equations one recovery runs through, and the shards that
/// supplied them.
///
/// The two halves are index-aligned and only useful together: row `i` of
/// `block` is the equation `shards[i]` contributes.
struct Selection {
    /// The `k × k` block of the unified matrix. Its rows are independent, so it
    /// inverts.
    block: Vec<Vec<u8>>,
    /// Bytes of the shards those rows belong to.
    shards: Vec<Vec<u8>>,
}

/// `matrix` with rows and columns swapped.
///
/// Called only on the square block of [`Selection`], which `solver::submatrix`
/// has already made rectangular. Were that ever to stop holding, the result
/// comes out short and `solver::solve` rejects it, rather than anything going
/// quietly wrong.
fn transpose(matrix: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let width = matrix.first().map_or(0, Vec::len);
    (0..width)
        .map(|column| matrix.iter().filter_map(|row| row.get(column).copied()).collect())
        .collect()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::lrc::LrcCodec;
    use crate::rs::split_data_shards;
    use crate::types::{CodecEngine, EcParity, EcProfileSnapshot, EncodedChunk};

    /// A profile, its solver, one encoded chunk and the data shards that chunk
    /// came from: everything a loss pattern is applied to.
    struct Fixture {
        profile: EcProfileSnapshot,
        local: u8,
        global: u8,
        solver: LrcSolver,
        encoded: EncodedChunk,
        data: Vec<Vec<u8>>,
    }

    impl Fixture {
        fn new(profile: EcProfileSnapshot, bytes_per_shard: u32) -> Self {
            let EcParity::Lrc { local, global } = profile.parity else {
                unreachable!("an LRC profile");
            };
            // The payload EC2-100 verified its matrix against.
            let input = (0..bytes_per_shard * u32::from(profile.k))
                .map(|idx| ((idx * 37 + 11) % 251) as u8)
                .collect::<Vec<_>>();
            let codec = LrcCodec::new(profile.clone()).unwrap();
            Self {
                solver: LrcSolver::new(profile.k, local, global).unwrap(),
                encoded: codec.encode_chunk(&input, 0).unwrap(),
                data: split_data_shards(&input, profile.k).unwrap(),
                profile,
                local,
                global,
            }
        }

        /// The shards that survive a loss pattern.
        fn survivors(&self, lost: &[bool]) -> Vec<ShardCandidate> {
            self.encoded
                .shards
                .iter()
                .filter(|shard| !lost[usize::from(shard.slot_index)])
                .map(|shard| ShardCandidate {
                    slot_index: shard.slot_index,
                    data: shard.data.clone(),
                })
                .collect()
        }

        fn lost_flags(&self, slots: &[u8]) -> Vec<bool> {
            (0..self.profile.total_shards()).map(|slot| slots.contains(&slot)).collect()
        }

        /// What the decoder of EC v1 makes of the same pattern: the two
        /// fast stages, and nothing beyond them.
        fn two_stage_decodes(&self, lost: &[bool]) -> bool {
            LrcCodec::fast_path_after_loss(self.profile.k, self.local, self.global, lost).recovers()
        }

        fn recover(&self, lost: &[bool]) -> Result<Vec<Vec<u8>>, CodecError> {
            self.solver.recover_data_shards(&self.survivors(lost))
        }
    }

    fn lrc_profiles() -> [EcProfileSnapshot; 4] {
        [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
            // `k / local = 1`: the local parity of a one-shard group repeats
            // that shard's own row. A degenerate shape, and the matrix still
            // has to describe it.
            EcProfileSnapshot::new_lrc("custom-small-groups", 4, 4, 2).unwrap(),
        ]
    }

    /// Count, spread by number of shards lost, and sum of masks — the shape the
    /// records in EC2-082 and EC2-102 are written in.
    fn assert_fingerprint(
        profile: &str,
        what: &str,
        masks: &[u32],
        slots: usize,
        count: usize,
        by_size: &[usize],
        mask_sum: u64,
    ) {
        let mut spread = vec![0usize; slots + 1];
        for mask in masks {
            spread[mask.count_ones() as usize] += 1;
        }
        assert_eq!(masks.len(), count, "{profile} {what} count");
        assert_eq!(spread, by_size, "{profile} {what} by size");
        assert_eq!(
            masks.iter().map(|mask| u64::from(*mask)).sum::<u64>(),
            mask_sum,
            "{profile} {what} mask sum"
        );
    }

    /// The prediction EC2-102 wrote down before this code existed: how many
    /// loss patterns stay unrecoverable, how they spread over the number of
    /// shards lost, and the sum of their masks. The sum is what tells "the
    /// right number of patterns" apart from "the right patterns".
    struct Fingerprint {
        unrecoverable: usize,
        unrecoverable_by_size: &'static [usize],
        unrecoverable_mask_sum: u64,
        gained: usize,
        gained_by_size: &'static [usize],
        gained_mask_sum: u64,
    }

    /// `EC2-LRC-02-001`. Every row is held against what `encode_chunk` actually
    /// stored in its slot: the row of slot `s` applied to the data shards must
    /// reproduce that shard byte for byte. Without this the solver could be
    /// perfectly consistent with itself and still wrong about the codec.
    #[test]
    fn every_row_reproduces_the_shard_the_codec_encoded() {
        for profile in lrc_profiles() {
            let fixture = Fixture::new(profile, 8);
            assert_eq!(
                fixture.encoded.shards.len(),
                usize::from(fixture.profile.total_shards())
            );
            for shard in &fixture.encoded.shards {
                let row = fixture.solver.row(shard.slot_index).unwrap();
                assert_eq!(row.len(), usize::from(fixture.profile.k));
                assert_eq!(
                    combine(row, &fixture.data),
                    shard.data.to_vec(),
                    "{} slot {}",
                    fixture.profile.name,
                    shard.slot_index
                );
            }
            assert!(fixture.solver.row(fixture.profile.total_shards()).is_err());
        }
    }

    /// `EC2-101` and `EC2-103`, the gain and its boundary. The first `g + 1`
    /// data shards of a group are more than global parity alone can carry, so
    /// the two-stage decoder refuses them, while the unified system solves them
    /// through the group's local parity. Lose that one local parity shard as
    /// well and the pattern really is unrecoverable — the pair shows an
    /// operator that the whole difference is a single shard.
    #[test]
    fn a_group_past_the_global_budget_is_solved_only_while_its_local_parity_lives() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let fixture = Fixture::new(profile, 8);
            let name = &fixture.profile.name;
            let per_group = fixture.profile.k / fixture.local;
            assert!(per_group > fixture.global, "{name} has nothing to gain");

            for group in 0..fixture.local {
                let start = group * per_group;
                let slots = (start..=start + fixture.global).collect::<Vec<u8>>();

                let lost = fixture.lost_flags(&slots);
                assert!(
                    !fixture.two_stage_decodes(&lost),
                    "{name} {slots:?} is not a gain: the fast path already solves it"
                );
                assert_eq!(
                    fixture.recover(&lost).unwrap(),
                    fixture.data,
                    "{name} {slots:?}"
                );

                let mut twin = slots.clone();
                twin.push(fixture.profile.k + group);
                let lost = fixture.lost_flags(&twin);
                assert!(
                    matches!(
                        fixture.recover(&lost),
                        Err(CodecError::NotRecoverable { .. })
                    ),
                    "{name} {twin:?} has more unknowns than equations and must be refused"
                );
            }
        }
    }

    /// `EC2-101`: two data shards of one group together with a global parity
    /// shard. Not a whole group, so the shape differs from the one above — two
    /// unknowns against the group's local parity and the one surviving global
    /// parity. The two-stage decoder sees two missing data shards, one live
    /// global parity, and gives up.
    #[test]
    fn data_loss_mixed_with_a_global_parity_loss_is_solved() {
        let fixture = Fixture::new(EcProfileSnapshot::lrc_standard(), 8);
        let lost = fixture.lost_flags(&[0, 1, 8]);
        assert!(!fixture.two_stage_decodes(&lost));
        assert_eq!(fixture.recover(&lost).unwrap(), fixture.data);
    }

    /// Every loss pattern of every standard LRC profile — 128, 1 024 and
    /// 16 384 of them — against four separate facts: the solver never recovers
    /// less than the two-stage decoder, never reports success while returning
    /// bytes that differ from the original, agrees on every pattern with the
    /// shards-free predicate placement plans by, and lands exactly on the counts
    /// EC2-102 recorded before this code was written.
    #[test]
    fn every_loss_pattern_lands_on_the_fingerprint_recorded_in_ec2_102() {
        for (profile, expected) in [
            (
                EcProfileSnapshot::lrc_small(),
                Fingerprint {
                    unrecoverable: 72,
                    unrecoverable_by_size: &[0, 0, 0, 8, 35, 21, 7, 1],
                    unrecoverable_mask_sum: 5_907,
                    gained: 8,
                    gained_by_size: &[0, 0, 2, 6, 0, 0, 0, 0],
                    gained_mask_sum: 123,
                },
            ),
            (
                EcProfileSnapshot::lrc_standard(),
                Fingerprint {
                    unrecoverable: 669,
                    unrecoverable_by_size: &[0, 0, 0, 0, 31, 252, 210, 120, 45, 10, 1],
                    unrecoverable_mask_sum: 408_801,
                    gained: 96,
                    gained_by_size: &[0, 0, 0, 14, 82, 0, 0, 0, 0, 0, 0],
                    gained_mask_sum: 28_968,
                },
            ),
            (
                EcProfileSnapshot::lrc_dense(),
                Fingerprint {
                    unrecoverable: 15_056,
                    unrecoverable_by_size: &[
                        0, 0, 0, 0, 143, 2002, 3003, 3432, 3003, 2002, 1001, 364, 91, 14, 1,
                    ],
                    unrecoverable_mask_sum: 129_023_993,
                    gained: 617,
                    gained_by_size: &[0, 0, 0, 60, 557, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                    gained_mask_sum: 2_164_888,
                },
            ),
        ] {
            // Four-byte shards: the patterns are what is under test, not the
            // arithmetic on long buffers, and `lrc_dense` alone has 16 384.
            let fixture = Fixture::new(profile, 4);
            let name = &fixture.profile.name;
            let slots = usize::from(fixture.profile.total_shards());
            let mut unrecoverable = Vec::new();
            let mut gained = Vec::new();

            for mask in 0u32..(1 << slots) {
                let lost = (0..slots).map(|slot| mask & (1 << slot) != 0).collect::<Vec<_>>();
                let two_stage = fixture.two_stage_decodes(&lost);
                let recovered = fixture.recover(&lost);
                // Placement plans by the predicate and never sees the shards,
                // so a predicate that drifted from the decoder would spread
                // data over a durability that no longer exists.
                assert_eq!(
                    fixture.solver.recovers_after_loss(&lost),
                    recovered.is_ok(),
                    "{name} mask {mask:#b}: the predicate and the recovery disagree"
                );
                match recovered {
                    Ok(data) => {
                        // Compared against the payload, not against a shard
                        // checksum: a checksum agrees with whatever bytes it
                        // was handed, so it cannot tell recovered data from
                        // convincing rubbish.
                        assert_eq!(data, fixture.data, "{name} mask {mask:#b}");
                        if !two_stage {
                            gained.push(mask);
                        }
                    }
                    Err(error) => {
                        assert!(
                            !two_stage,
                            "{name} mask {mask:#b}: the fast path solves this and the solver did not: {error}"
                        );
                        assert!(
                            matches!(error, CodecError::NotRecoverable { .. }),
                            "{name} mask {mask:#b}: lost data must read as not recoverable, got {error}"
                        );
                        unrecoverable.push(mask);
                    }
                }
            }

            assert_fingerprint(
                name,
                "unrecoverable",
                &unrecoverable,
                slots,
                expected.unrecoverable,
                expected.unrecoverable_by_size,
                expected.unrecoverable_mask_sum,
            );
            assert_fingerprint(
                name,
                "gained",
                &gained,
                slots,
                expected.gained,
                expected.gained_by_size,
                expected.gained_mask_sum,
            );
        }
    }

    /// `EC2-101`: where a group holds no more data shards than there are global
    /// parity shards (`k / l <= g`), the unified system adds nothing — global
    /// parity already covers everything local parity could contribute. The
    /// solver must then agree with the two-stage decoder on every pattern,
    /// neither more nor less.
    #[test]
    fn a_profile_with_small_groups_gains_nothing() {
        let profile = EcProfileSnapshot::new_lrc("custom-small-groups", 4, 4, 2).unwrap();
        let fixture = Fixture::new(profile, 4);
        let slots = usize::from(fixture.profile.total_shards());

        for mask in 0u32..(1 << slots) {
            let lost = (0..slots).map(|slot| mask & (1 << slot) != 0).collect::<Vec<_>>();
            let recovered = fixture.recover(&lost);
            assert_eq!(
                recovered.is_ok(),
                fixture.two_stage_decodes(&lost),
                "mask {mask:#b}"
            );
            if let Ok(data) = recovered {
                assert_eq!(data, fixture.data, "mask {mask:#b}");
            }
        }
    }

    /// `EC2-LRC-02-004`. A whole local group — its data shards and its local
    /// parity — leaves `k / l` unknowns against `g` global equations, and all
    /// three standard profiles have `k / l > g`. Many vectors fit and nothing
    /// tells them apart, so the solver says so instead of returning one.
    #[test]
    fn a_whole_local_group_is_refused_rather_than_guessed() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let fixture = Fixture::new(profile, 8);
            let per_group = fixture.profile.k / fixture.local;
            for group in 0..fixture.local {
                let start = group * per_group;
                let mut slots = (start..start + per_group).collect::<Vec<u8>>();
                slots.push(fixture.profile.k + group);
                let lost = fixture.lost_flags(&slots);
                assert!(
                    matches!(
                        fixture.recover(&lost),
                        Err(CodecError::NotRecoverable { .. })
                    ),
                    "{} {slots:?}",
                    fixture.profile.name
                );
            }
        }
    }

    /// Fewer shards than unknowns cannot determine the data however they are
    /// arranged, and the refusal names the shortfall rather than just failing.
    #[test]
    fn fewer_shards_than_unknowns_are_refused() {
        let fixture = Fixture::new(EcProfileSnapshot::lrc_standard(), 8);
        let lost = fixture.lost_flags(&[0, 1, 2, 3, 4]);
        let error = fixture.recover(&lost).unwrap_err();
        assert!(matches!(error, CodecError::NotRecoverable { .. }));
        assert!(
            error.to_string().contains("for 6 unknown(s)"),
            "the refusal should say what was short: {error}"
        );
    }

    /// A shard set that cannot describe a system is rejected as such, rather
    /// than being reported as lost data.
    #[test]
    fn an_unusable_shard_set_is_rejected() {
        let fixture = Fixture::new(EcProfileSnapshot::lrc_small(), 8);
        let intact = fixture.survivors(&fixture.lost_flags(&[]));

        assert!(matches!(
            fixture.solver.recover_data_shards(&[]),
            Err(CodecError::NotRecoverable { .. })
        ));

        let mut duplicated = intact.clone();
        duplicated.push(intact[0].clone());
        assert!(matches!(
            fixture.solver.recover_data_shards(&duplicated),
            Err(CodecError::InvalidShardSet { .. })
        ));

        let mut outside = intact.clone();
        outside.push(ShardCandidate {
            slot_index: fixture.profile.total_shards(),
            data: intact[0].data.clone(),
        });
        assert!(matches!(
            fixture.solver.recover_data_shards(&outside),
            Err(CodecError::InvalidShardSet { .. })
        ));

        // Uneven shards would send the combination off the end of the shortest
        // one, so they are turned away before any arithmetic starts.
        let mut ragged = intact.clone();
        ragged[1].data = Bytes::from_static(b"short");
        assert!(matches!(
            fixture.solver.recover_data_shards(&ragged),
            Err(CodecError::InvalidShardSet { .. })
        ));
    }

    #[test]
    fn an_impossible_shape_has_no_matrix() {
        assert!(LrcSolver::new(0, 2, 1).is_err());
        assert!(LrcSolver::new(4, 0, 1).is_err());
        assert!(LrcSolver::new(4, 2, 0).is_err());
        // Five data shards do not split into two equal groups.
        assert!(LrcSolver::new(5, 2, 1).is_err());
    }

    /// `recover_slot` over every loss pattern of every standard profile and
    /// every slot that pattern took away — 448, 5 120 and 114 688 pairs. When
    /// the chunk is recoverable the rebuilt shard has to be the encoder's own
    /// bytes; when it is not, the repair has to refuse. This is the pairing
    /// EC2-101 counted its repair reach over, and what `repair_shard` will rest
    /// on in phase 3.
    #[test]
    fn recover_slot_rebuilds_the_encoders_own_bytes_or_refuses() {
        for profile in [
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let fixture = Fixture::new(profile, 4);
            let name = &fixture.profile.name;
            let slots = usize::from(fixture.profile.total_shards());

            for mask in 0u32..(1 << slots) {
                let lost = (0..slots).map(|slot| mask & (1 << slot) != 0).collect::<Vec<_>>();
                let survivors = fixture.survivors(&lost);
                let recoverable = fixture.solver.recovers_after_loss(&lost);
                for slot in (0..slots).filter(|slot| lost[*slot]) {
                    let slot = u8::try_from(slot).unwrap();
                    let rebuilt = fixture.solver.recover_slot(&survivors, slot);
                    assert_eq!(
                        rebuilt.is_ok(),
                        recoverable,
                        "{name} mask {mask:#b} slot {slot}"
                    );
                    if let Ok(bytes) = rebuilt {
                        assert_eq!(
                            bytes.as_slice(),
                            fixture.encoded.shards[usize::from(slot)].data.as_ref(),
                            "{name} mask {mask:#b} slot {slot}"
                        );
                    }
                }
            }
        }
    }

    /// The solver evaluates a slot's row rather than looking the shard up, so a
    /// slot that is still present comes back with the bytes it already holds.
    /// That is what makes the row a definition instead of a lookup, and it is
    /// harmless on its own: `repair_shard` establishes that a slot is absent
    /// before it repairs anything.
    #[test]
    fn a_present_slot_is_evaluated_rather_than_looked_up() {
        let fixture = Fixture::new(EcProfileSnapshot::lrc_standard(), 8);
        let intact = fixture.survivors(&fixture.lost_flags(&[]));
        for shard in &fixture.encoded.shards {
            let rebuilt = fixture.solver.recover_slot(&intact, shard.slot_index).unwrap();
            assert_eq!(
                rebuilt.as_slice(),
                shard.data.as_ref(),
                "slot {}",
                shard.slot_index
            );
        }
        assert!(fixture.solver.recover_slot(&intact, fixture.profile.total_shards()).is_err());
    }

    /// The convention `LossTolerance` asks in, which phase 3 will route through
    /// this predicate: a slot past the end of `lost` counts as present.
    #[test]
    fn slots_past_the_end_of_the_pattern_count_as_present() {
        let solver = LrcSolver::new(6, 2, 2).unwrap();
        assert!(solver.recovers_after_loss(&[]));
        assert!(solver.recovers_after_loss(&[true, true]));
        // All three data shards of group 0, its local parity alive. The
        // two-stage decoder refuses this one — it is the gain of EC2-101.
        assert!(solver.recovers_after_loss(&[true, true, true]));
        assert!(!LrcCodec::fast_path_after_loss(6, 2, 2, &[true, true, true]).recovers());
        // Those three plus the group's local parity: six unknowns against five
        // equations, and no decoder recovers it.
        assert!(!solver.recovers_after_loss(&[true, true, true, false, false, false, true]));
    }
}
