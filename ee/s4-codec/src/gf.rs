// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Small GF(256) helper module for Reed-Solomon math.
//!
//! The [`solver`] submodule adds what the LRC decoder needs on top of the field
//! arithmetic: submatrix extraction, rank, and solving a rectangular system.

use crate::types::CodecError;

const REDUCTION_POLY: u8 = 0x1d;

pub(crate) fn add(a: u8, b: u8) -> u8 {
    a ^ b
}

pub(crate) fn mul(mut a: u8, mut b: u8) -> u8 {
    let mut result = 0;
    while b != 0 {
        if b & 1 != 0 {
            result ^= a;
        }
        let carry = a & 0x80 != 0;
        a <<= 1;
        if carry {
            a ^= REDUCTION_POLY;
        }
        b >>= 1;
    }
    result
}

pub(crate) fn pow(mut value: u8, mut exponent: usize) -> u8 {
    let mut result = 1;
    while exponent != 0 {
        if exponent & 1 != 0 {
            result = mul(result, value);
        }
        value = mul(value, value);
        exponent >>= 1;
    }
    result
}

pub(crate) fn inv(value: u8) -> Result<u8, CodecError> {
    if value == 0 {
        return Err(CodecError::Arithmetic {
            reason: "zero has no multiplicative inverse in GF(256)".to_string(),
        });
    }
    Ok(pow(value, 254))
}

pub(crate) fn matrix_multiply(
    left: &[Vec<u8>],
    right: &[Vec<u8>],
) -> Result<Vec<Vec<u8>>, CodecError> {
    if left.is_empty() || right.is_empty() {
        return Err(CodecError::Arithmetic {
            reason: "cannot multiply empty matrices".to_string(),
        });
    }
    let inner = right.len();
    if left.iter().any(|row| row.len() != inner) {
        return Err(CodecError::Arithmetic {
            reason: "matrix multiply dimension mismatch".to_string(),
        });
    }
    let columns = right[0].len();
    if columns == 0 || right.iter().any(|row| row.len() != columns) {
        return Err(CodecError::Arithmetic {
            reason: "right matrix is ragged or empty".to_string(),
        });
    }

    let mut result = vec![vec![0; columns]; left.len()];
    for (row_idx, left_row) in left.iter().enumerate() {
        for col_idx in 0..columns {
            let mut value = 0;
            for idx in 0..inner {
                value = add(value, mul(left_row[idx], right[idx][col_idx]));
            }
            result[row_idx][col_idx] = value;
        }
    }
    Ok(result)
}

pub(crate) fn invert_matrix(matrix: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, CodecError> {
    let size = matrix.len();
    if size == 0 {
        return Err(CodecError::Arithmetic {
            reason: "cannot invert an empty matrix".to_string(),
        });
    }
    if matrix.iter().any(|row| row.len() != size) {
        return Err(CodecError::Arithmetic {
            reason: "matrix must be square".to_string(),
        });
    }

    let mut augmented = vec![vec![0; size * 2]; size];
    for row in 0..size {
        for col in 0..size {
            augmented[row][col] = matrix[row][col];
        }
        augmented[row][size + row] = 1;
    }

    for col in 0..size {
        let pivot = (col..size).find(|row| augmented[*row][col] != 0);
        let Some(pivot) = pivot else {
            return Err(CodecError::Arithmetic {
                reason: "matrix is singular".to_string(),
            });
        };
        if pivot != col {
            augmented.swap(pivot, col);
        }

        let inverse = inv(augmented[col][col])?;
        for value in &mut augmented[col] {
            *value = mul(*value, inverse);
        }

        for row in 0..size {
            if row == col {
                continue;
            }
            let factor = augmented[row][col];
            if factor == 0 {
                continue;
            }
            let pivot_row = augmented[col].clone();
            for (target, pivot) in augmented[row].iter_mut().zip(pivot_row.iter()) {
                *target = add(*target, mul(factor, *pivot));
            }
        }
    }

    Ok(augmented.into_iter().map(|row| row[size..].to_vec()).collect())
}

/// Linear algebra over GF(2^8) for the unified LRC solver.
///
/// Reed-Solomon decoding never needed any of this. Its matrix is built so that
/// every square block of it is invertible, so the decoder takes any `k` rows,
/// inverts them and moves on. The LRC decoder cannot: its rows come from two
/// different parity constructions, and whether a given set of them determines
/// the data is exactly the question to answer. Answering it takes a rank, and
/// refusing to answer takes a way of saying why — see decision EC2-100.
pub(crate) mod solver {
    use std::fmt;

    use super::{add, inv, mul};
    use crate::types::CodecError;

    /// Why a system over GF(2^8) does not have exactly one solution.
    ///
    /// The distinction is the point of the type. To a caller that only hears
    /// "failed", too few equations and contradicting equations look the same,
    /// while they say different things about the data: the first means too much
    /// was lost, the second means what is left disagrees with itself.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum SolveFailure {
        /// The input cannot describe a system at all: empty, ragged, or with
        /// fewer columns than the unknowns it is asked about.
        Malformed(String),
        /// Fewer independent equations than unknowns. Many vectors satisfy the
        /// system and nothing tells them apart, so there is no answer to give.
        RankDeficient {
            /// Independent equations found.
            rank: usize,
            /// Unknowns the system has.
            unknowns: usize,
        },
        /// The equations contradict each other, so no vector satisfies them all.
        /// Named by the equation, in the order the caller gave them, whose
        /// reduced form claims `0 = value`.
        Inconsistent {
            /// Index of the contradicting equation in the input.
            equation: usize,
        },
    }

    impl fmt::Display for SolveFailure {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Malformed(reason) => write!(formatter, "unusable system: {reason}"),
                Self::RankDeficient { rank, unknowns } => write!(
                    formatter,
                    "system has {rank} independent equation(s) for {unknowns} unknown(s)"
                ),
                Self::Inconsistent { equation } => {
                    write!(formatter, "equation {equation} contradicts the others")
                }
            }
        }
    }

    impl From<SolveFailure> for CodecError {
        fn from(failure: SolveFailure) -> Self {
            let reason = failure.to_string();
            match failure {
                // A malformed system is a caller bug, too few equations is a
                // loss the caller has to live with, and a contradiction means
                // the inputs disagree — three different errors on purpose.
                SolveFailure::Malformed(_) => Self::Arithmetic { reason },
                SolveFailure::RankDeficient { .. } => Self::NotRecoverable { reason },
                SolveFailure::Inconsistent { .. } => Self::InvalidShardSet { reason },
            }
        }
    }

    /// A matrix in reduced row echelon form, and where its rows came from.
    struct Reduction {
        /// Reduced rows: the pivot rows first, all-zero rows after them. Wider
        /// than the coefficients when a right-hand side was appended.
        rows: Vec<Vec<u8>>,
        /// Column each pivot row leads with, in order.
        pivot_columns: Vec<usize>,
        /// Index the row in this position had in the input.
        source_rows: Vec<usize>,
    }

    impl Reduction {
        fn rank(&self) -> usize {
            self.pivot_columns.len()
        }
    }

    /// Reduces `rows` to reduced row echelon form, looking for pivots in the
    /// first `width` columns only.
    ///
    /// Columns past `width` are carried along by every row operation without
    /// ever holding a pivot, which is how a right-hand side rides through the
    /// reduction. Rows from `rank` onwards end up zero across all `width`
    /// coefficients, so anything left in their trailing columns is an equation
    /// that says `0 = value`.
    ///
    /// The rows the pivots came from are linearly independent in the input:
    /// pivot rows only ever receive multiples of earlier pivot rows, so the
    /// reduced pivot rows are an invertible combination of exactly those input
    /// rows, and they are independent because each leads in its own column.
    fn reduce(rows: &[Vec<u8>], width: usize) -> Result<Reduction, CodecError> {
        if rows.is_empty() {
            return Err(CodecError::Arithmetic {
                reason: "cannot reduce a matrix without rows".to_string(),
            });
        }
        if width == 0 {
            return Err(CodecError::Arithmetic {
                reason: "a system needs at least one unknown".to_string(),
            });
        }
        let columns = rows[0].len();
        if columns < width || rows.iter().any(|row| row.len() != columns) {
            return Err(CodecError::Arithmetic {
                reason: format!(
                    "every row must hold the same {width} coefficient(s) or more, \
                     got rows of {:?}",
                    rows.iter().map(Vec::len).collect::<Vec<_>>()
                ),
            });
        }

        let mut reduced = rows.to_vec();
        let mut source_rows = (0..reduced.len()).collect::<Vec<_>>();
        let mut pivot_columns = Vec::new();

        for column in 0..width {
            let pivot_row = pivot_columns.len();
            if pivot_row == reduced.len() {
                break;
            }
            let Some(found) = (pivot_row..reduced.len()).find(|row| reduced[*row][column] != 0)
            else {
                // No pivot here: the column is free, and every row below keeps
                // its zero in it.
                continue;
            };
            reduced.swap(pivot_row, found);
            source_rows.swap(pivot_row, found);

            let inverse = inv(reduced[pivot_row][column])?;
            for value in &mut reduced[pivot_row] {
                *value = mul(*value, inverse);
            }
            let pivot = reduced[pivot_row].clone();
            for (index, row) in reduced.iter_mut().enumerate() {
                if index == pivot_row {
                    continue;
                }
                let factor = row[column];
                if factor == 0 {
                    continue;
                }
                for (target, coefficient) in row.iter_mut().zip(pivot.iter()) {
                    *target = add(*target, mul(factor, *coefficient));
                }
            }
            pivot_columns.push(column);
        }

        Ok(Reduction {
            rows: reduced,
            pivot_columns,
            source_rows,
        })
    }

    /// Copies the listed rows and columns of `matrix` into a smaller matrix.
    ///
    /// Only the named entries are copied, so picking `k` rows of a tall matrix
    /// costs `k` rows rather than a copy of the whole thing.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Arithmetic`] when either list is empty, when an
    /// index falls outside the matrix, or when the matrix is ragged.
    pub(crate) fn submatrix(
        matrix: &[Vec<u8>],
        rows: &[usize],
        columns: &[usize],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        if rows.is_empty() || columns.is_empty() {
            return Err(CodecError::Arithmetic {
                reason: "a submatrix needs at least one row and one column".to_string(),
            });
        }
        let width = matrix.first().map_or(0, Vec::len);
        rows.iter()
            .map(|row_index| {
                let row = matrix.get(*row_index).ok_or_else(|| CodecError::Arithmetic {
                    reason: format!(
                        "row {row_index} is outside a matrix of {} row(s)",
                        matrix.len()
                    ),
                })?;
                if row.len() != width {
                    return Err(CodecError::Arithmetic {
                        reason: "matrix is ragged".to_string(),
                    });
                }
                columns
                    .iter()
                    .map(|column| {
                        row.get(*column).copied().ok_or_else(|| CodecError::Arithmetic {
                            reason: format!(
                                "column {column} is outside a matrix of {width} column(s)"
                            ),
                        })
                    })
                    .collect()
            })
            .collect()
    }

    /// Number of linearly independent rows of `matrix`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Arithmetic`] when the matrix is empty or ragged.
    pub(crate) fn rank(matrix: &[Vec<u8>]) -> Result<usize, CodecError> {
        let width = matrix.first().map_or(0, Vec::len);
        Ok(reduce(matrix, width)?.rank())
    }

    /// Picks `wanted` rows of `matrix` that are linearly independent, by their
    /// index in `matrix` and in ascending order.
    ///
    /// This is what a decoder asks before it inverts anything: which of the
    /// shards it still has carry enough independent equations to determine the
    /// data.
    ///
    /// # Errors
    ///
    /// Returns [`SolveFailure::RankDeficient`] when the matrix holds fewer than
    /// `wanted` independent rows, and [`SolveFailure::Malformed`] when it is not
    /// a matrix.
    pub(crate) fn independent_rows(
        matrix: &[Vec<u8>],
        wanted: usize,
    ) -> Result<Vec<usize>, SolveFailure> {
        let width = matrix.first().map_or(0, Vec::len);
        let reduction =
            reduce(matrix, width).map_err(|error| SolveFailure::Malformed(error.to_string()))?;
        if reduction.rank() < wanted {
            return Err(SolveFailure::RankDeficient {
                rank: reduction.rank(),
                unknowns: wanted,
            });
        }
        let mut rows = reduction.source_rows[..wanted].to_vec();
        rows.sort_unstable();
        Ok(rows)
    }

    /// Solves `matrix · x = rhs` over GF(2^8) for a rectangular `matrix`.
    ///
    /// Returns the one vector that satisfies every equation, or an error saying
    /// why there is no such single vector. There is no third outcome: a system
    /// short of equations is refused rather than completed with a guess, which
    /// would be a plausible-looking answer that is almost certainly wrong.
    ///
    /// A contradiction is reported ahead of a missing equation when a system has
    /// both, because it is the more specific fact: it says the inputs disagree,
    /// not merely that some are gone.
    ///
    /// # Errors
    ///
    /// Returns [`SolveFailure::Inconsistent`], [`SolveFailure::RankDeficient`]
    /// or [`SolveFailure::Malformed`].
    pub(crate) fn solve(matrix: &[Vec<u8>], rhs: &[u8]) -> Result<Vec<u8>, SolveFailure> {
        if rhs.len() != matrix.len() {
            return Err(SolveFailure::Malformed(format!(
                "expected {} right-hand side value(s), got {}",
                matrix.len(),
                rhs.len()
            )));
        }
        let width = matrix.first().map_or(0, Vec::len);
        let augmented = matrix
            .iter()
            .zip(rhs)
            .map(|(row, value)| {
                let mut row = row.clone();
                row.push(*value);
                row
            })
            .collect::<Vec<_>>();
        let reduction = reduce(&augmented, width)
            .map_err(|error| SolveFailure::Malformed(error.to_string()))?;

        for (position, row) in reduction.rows.iter().enumerate().skip(reduction.rank()) {
            if row[width] != 0 {
                return Err(SolveFailure::Inconsistent {
                    equation: reduction.source_rows[position],
                });
            }
        }
        if reduction.rank() < width {
            return Err(SolveFailure::RankDeficient {
                rank: reduction.rank(),
                unknowns: width,
            });
        }

        let mut solution = vec![0; width];
        for (position, column) in reduction.pivot_columns.iter().enumerate() {
            solution[*column] = reduction.rows[position][width];
        }
        Ok(solution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gf_inverse_round_trips_non_zero_values() {
        for value in 1..=u8::MAX {
            let inverse = inv(value).unwrap();
            assert_eq!(mul(value, inverse), 1);
        }
    }

    #[test]
    fn matrix_inverse_produces_identity() {
        let matrix = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 10]];
        let inverse = invert_matrix(&matrix).unwrap();
        let identity = matrix_multiply(&matrix, &inverse).unwrap();
        assert_eq!(identity, vec![vec![1, 0, 0], vec![0, 1, 0], vec![0, 0, 1]]);
    }

    mod solver_tests {
        use super::super::solver::{independent_rows, rank, solve, submatrix, SolveFailure};
        use super::*;

        /// One equation applied to a candidate solution.
        fn apply(row: &[u8], values: &[u8]) -> u8 {
            row.iter().zip(values).fold(0, |acc, (coefficient, value)| {
                add(acc, mul(*coefficient, *value))
            })
        }

        fn right_hand_side(matrix: &[Vec<u8>], solution: &[u8]) -> Vec<u8> {
            matrix.iter().map(|row| apply(row, solution)).collect()
        }

        /// Deterministic xorshift. Random inputs find what hand-picked ones
        /// miss, but a failing case has to be reproducible, so there is a seed
        /// rather than a source of entropy.
        struct Rng(u64);

        impl Rng {
            fn byte(&mut self) -> u8 {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                (self.0 >> 33) as u8
            }

            fn below(&mut self, limit: usize) -> usize {
                usize::from(self.byte()) % limit
            }
        }

        /// Whether some square block of `matrix` inverts, answered with the
        /// inverter that predates this module. A system is solvable exactly
        /// when one does, so this checks the solver against code it shares
        /// nothing with.
        fn some_square_block_inverts(matrix: &[Vec<u8>], unknowns: usize) -> bool {
            let rows = matrix.len();
            (0u32..1 << rows)
                .filter(|mask| mask.count_ones() as usize == unknowns)
                .any(|mask| {
                    let block = (0..rows)
                        .filter(|row| mask & (1 << row) != 0)
                        .map(|row| matrix[row].clone())
                        .collect::<Vec<_>>();
                    invert_matrix(&block).is_ok()
                })
        }

        #[test]
        fn submatrix_copies_only_the_requested_rows_and_columns() {
            let matrix = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]];
            assert_eq!(
                submatrix(&matrix, &[0, 2], &[1, 2]).unwrap(),
                vec![vec![2, 3], vec![8, 9]]
            );
            assert_eq!(submatrix(&matrix, &[1], &[0]).unwrap(), vec![vec![4]]);
        }

        #[test]
        fn submatrix_refuses_what_it_cannot_pick() {
            let matrix = vec![vec![1, 2, 3], vec![4, 5, 6]];
            assert!(submatrix(&matrix, &[0, 2], &[0]).is_err());
            assert!(submatrix(&matrix, &[0], &[3]).is_err());
            assert!(submatrix(&matrix, &[], &[0]).is_err());
            assert!(submatrix(&matrix, &[0], &[]).is_err());
        }

        #[test]
        fn rank_counts_independent_rows_only() {
            assert_eq!(rank(&[vec![1, 0], vec![0, 1]]).unwrap(), 2);
            // The second row is the first one scaled: it adds nothing.
            assert_eq!(rank(&[vec![1, 0], vec![2, 0]]).unwrap(), 1);
            // The third row is the sum of the first two.
            assert_eq!(rank(&[vec![1, 1], vec![0, 1], vec![1, 0]]).unwrap(), 2);
            assert!(rank(&[]).is_err());
        }

        #[test]
        fn solving_a_square_system_agrees_with_the_matrix_inverse() {
            let matrix = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 10]];
            let expected = vec![11, 22, 33];
            let rhs = right_hand_side(&matrix, &expected);

            assert_eq!(solve(&matrix, &rhs).unwrap(), expected);

            let inverse = invert_matrix(&matrix).unwrap();
            let through_inverse = inverse.iter().map(|row| apply(row, &rhs)).collect::<Vec<_>>();
            assert_eq!(through_inverse, expected);
        }

        #[test]
        fn more_equations_than_unknowns_still_solve() {
            let mut matrix = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 10]];
            // A fourth equation that repeats what the first two already say.
            matrix.push(vec![5, 7, 5]);
            let expected = vec![9, 8, 7];
            let rhs = right_hand_side(&matrix, &expected);
            assert_eq!(solve(&matrix, &rhs).unwrap(), expected);
        }

        #[test]
        fn a_system_short_of_equations_is_refused_not_guessed() {
            // The third row is the sum of the first two, so three equations
            // carry only two.
            let matrix = vec![vec![1, 1, 0], vec![0, 1, 1], vec![1, 0, 1]];
            let rhs = right_hand_side(&matrix, &[3, 5, 7]);
            match solve(&matrix, &rhs) {
                Err(SolveFailure::RankDeficient { rank, unknowns }) => {
                    assert_eq!((rank, unknowns), (2, 3));
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
        }

        #[test]
        fn contradicting_equations_are_named() {
            // The same left-hand side twice with two different answers.
            let matrix = vec![vec![1, 1, 0], vec![0, 1, 1], vec![1, 1, 0]];
            match solve(&matrix, &[7, 9, 8]) {
                Err(SolveFailure::Inconsistent { equation }) => assert_eq!(equation, 2),
                other => panic!("expected a contradiction, got {other:?}"),
            }
        }

        #[test]
        fn a_system_that_is_not_a_system_is_rejected() {
            assert!(matches!(
                solve(&[vec![1, 0]], &[1, 2]),
                Err(SolveFailure::Malformed(_))
            ));
            assert!(matches!(
                solve(&[vec![1, 0], vec![1]], &[1, 2]),
                Err(SolveFailure::Malformed(_))
            ));
            assert!(matches!(solve(&[], &[]), Err(SolveFailure::Malformed(_))));
        }

        #[test]
        fn independent_rows_skips_rows_that_add_nothing() {
            let matrix = vec![vec![1, 1], vec![2, 2], vec![0, 1]];
            let chosen = independent_rows(&matrix, 2).unwrap();
            assert_eq!(chosen, vec![0, 2]);

            let block = chosen.iter().map(|row| matrix[*row].clone()).collect::<Vec<_>>();
            assert!(invert_matrix(&block).is_ok());
        }

        #[test]
        fn independent_rows_refuses_when_the_rank_is_too_low() {
            let matrix = vec![vec![1, 1], vec![2, 2]];
            match independent_rows(&matrix, 2) {
                Err(SolveFailure::RankDeficient { rank, unknowns }) => {
                    assert_eq!((rank, unknowns), (1, 2));
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
        }

        #[test]
        fn random_systems_solve_and_substitute_back_to_an_identity() {
            let mut rng = Rng(0x5eed_0f11_c0de_1234);
            let mut solved = 0;
            for _ in 0..400 {
                let unknowns = 2 + rng.below(4);
                let equations = unknowns + rng.below(3);
                let matrix = (0..equations)
                    .map(|_| (0..unknowns).map(|_| rng.byte()).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                let expected = (0..unknowns).map(|_| rng.byte()).collect::<Vec<_>>();
                let rhs = right_hand_side(&matrix, &expected);

                match solve(&matrix, &rhs) {
                    Ok(solution) => {
                        assert_eq!(right_hand_side(&matrix, &solution), rhs);
                        assert_eq!(solution, expected);
                        assert!(independent_rows(&matrix, unknowns).is_ok());
                        solved += 1;
                    }
                    Err(SolveFailure::RankDeficient {
                        rank,
                        unknowns: reported,
                    }) => {
                        assert_eq!(reported, unknowns);
                        assert!(rank < unknowns);
                        assert!(!some_square_block_inverts(&matrix, unknowns));
                    }
                    Err(other) => {
                        panic!("a system built from a real solution cannot be {other}")
                    }
                }
            }
            assert!(
                solved > 300,
                "random systems should mostly have full rank, solved {solved}"
            );
        }

        #[test]
        fn random_systems_short_of_equations_are_always_refused() {
            let mut rng = Rng(0x0bad_c0ff_ee00_1234);
            for _ in 0..200 {
                let unknowns = 3 + rng.below(3);
                let independent = 1 + rng.below(unknowns - 1);
                let basis = (0..independent)
                    .map(|_| (0..unknowns).map(|_| rng.byte()).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                // Every equation is a combination of the same few, so the rank
                // cannot reach the number of unknowns however many are added.
                let matrix = (0..unknowns + rng.below(3))
                    .map(|_| {
                        let mut row = vec![0; unknowns];
                        for source in &basis {
                            let weight = rng.byte();
                            for (target, coefficient) in row.iter_mut().zip(source) {
                                *target = add(*target, mul(weight, *coefficient));
                            }
                        }
                        row
                    })
                    .collect::<Vec<_>>();
                let expected = (0..unknowns).map(|_| rng.byte()).collect::<Vec<_>>();
                let rhs = right_hand_side(&matrix, &expected);

                match solve(&matrix, &rhs) {
                    Err(SolveFailure::RankDeficient {
                        rank,
                        unknowns: reported,
                    }) => {
                        assert_eq!(reported, unknowns);
                        assert!(rank <= independent);
                        assert!(!some_square_block_inverts(&matrix, unknowns));
                    }
                    other => panic!("expected a refusal, got {other:?}"),
                }
            }
        }

        #[test]
        fn random_contradictions_are_never_solved() {
            let mut rng = Rng(0xfeed_dead_beef_0042);
            for _ in 0..200 {
                let unknowns = 2 + rng.below(4);
                let equations = unknowns + 1;
                let mut matrix = (0..equations)
                    .map(|_| (0..unknowns).map(|_| rng.byte()).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                let expected = (0..unknowns).map(|_| rng.byte()).collect::<Vec<_>>();
                let mut rhs = right_hand_side(&matrix, &expected);

                // Repeat one equation with a different answer: whatever else the
                // system says, no vector satisfies both copies.
                let repeated = rng.below(equations);
                matrix.push(matrix[repeated].clone());
                rhs.push(rhs[repeated] ^ 1);

                match solve(&matrix, &rhs) {
                    Err(SolveFailure::Inconsistent { equation }) => {
                        assert!(equation < matrix.len());
                    }
                    other => panic!("expected a contradiction, got {other:?}"),
                }
            }
        }
    }
}
