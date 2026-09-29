// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Reference math that every codec backend must reproduce exactly.
//!
//! S4 has more than one way to compute erasure codes: the portable Rust engine
//! in this crate, and hardware accelerated engines built on top of it. They must
//! all produce byte-identical shards, otherwise a cluster running a mix of them
//! would treat healthy data as corrupt.
//!
//! This module exposes the small set of primitives an alternative backend needs
//! in order to match. It is deliberately narrow: it publishes the definition of
//! correctness, not the internals of the codec.
//!
//! Two kinds of items live here. The generator matrix and the field arithmetic
//! define what a matching backend must compute. Everything else — splitting a
//! chunk, validating a shard set, trimming padding, listing failure patterns —
//! is shared rather than redefined, because a backend that reimplemented these
//! could differ from the portable engine without touching a single coefficient.
//!
//! See decision EC2-002 in the EC v2 plans for why byte equality is a hard
//! requirement rather than a nice-to-have.

use crate::gf;
use crate::types::CodecError;

pub use crate::rs::{
    ensure_enough_candidates, ensure_slot_in_profile, ensure_slot_is_absent,
    reed_solomon_failure_matrix, reed_solomon_recovery_path, split_data_shards,
    systematic_generator_matrix, truncate_data_shards, validate_data_shards, ShardCandidate,
    ValidShardSet,
};

/// Multiplies two elements of GF(2^8) as S4 defines the field.
///
/// The field uses the reduction polynomial `0x1d`, that is
/// `x^8 + x^4 + x^3 + x^2 + 1`. An accelerated backend is only compatible if it
/// uses the same polynomial, so this function is the reference an FFI backend
/// is tested against over all 65536 input pairs.
#[must_use]
pub fn gf_mul(a: u8, b: u8) -> u8 {
    gf::mul(a, b)
}

/// Inverts a square matrix over GF(2^8).
///
/// Decoding picks the generator rows of the shards it actually has and inverts
/// that square block to get the data shards back. The inverse of a matrix is
/// unique, so any correct implementation over the same field would agree; the
/// routine is shared anyway so that a backend cannot quietly grow a second one
/// over a different field.
///
/// # Errors
///
/// Returns [`CodecError::Arithmetic`] when the matrix is empty, not square, or
/// singular.
pub fn invert_matrix(matrix: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, CodecError> {
    gf::invert_matrix(matrix)
}
