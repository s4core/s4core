// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Deliberate misbehaviour, for proving that a check can fail.
//!
//! A green byte-equality test only means something if it is capable of turning
//! red. The mixed-cluster acceptance script therefore arms the hook below,
//! which makes the accelerated engine encode with a *different* generator
//! matrix, and then demands that the comparison against the portable engine
//! fails. If it still passes, the check is a tautology and the whole ISA-L
//! result is worthless — so the script fails instead.
//!
//! The second hook here is not misbehaviour but an earlier edition of it:
//! [`lrc_fast_path_only`] holds a node's LRC recovery to the two fast paths, so
//! that one build can stand in for both sides of a rolling upgrade and the
//! acceptance script can watch a solver node and a pre-solver node meet over
//! the same shards.
//!
//! Nothing here is reachable in a normal process: every entry point refuses
//! unless `S4_EC_TEST_HOOKS_ENABLED` is set, and every hook starts disarmed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use crate::rs::{systematic_matrix_from_point, FIRST_VANDERMONDE_POINT};
use crate::types::CodecError;

/// Environment flag that has to be set before any hook can be armed.
pub const TEST_HOOKS_ENV: &str = "S4_EC_TEST_HOOKS_ENABLED";

/// Vandermonde starting point for the deliberately wrong matrix.
///
/// Any value other than [`FIRST_VANDERMONDE_POINT`] gives a valid Reed-Solomon
/// code whose parity coefficients differ from S4's. Keeping the matrix valid
/// and systematic matters: the object still encodes and decodes on the node
/// that wrote it, so the script observes a byte difference rather than a pile
/// of unrelated failures.
const FOREIGN_VANDERMONDE_POINT: u8 = FIRST_VANDERMONDE_POINT + 1;

static FOREIGN_MATRIX: AtomicBool = AtomicBool::new(false);

/// Arm or disarm the foreign-matrix hook.
///
/// Takes effect on the next codec built for **writing**. The EC runtime asks
/// for a hook-aware codec on its encode path only, so decode and repair keep
/// using the stored matrix even while this is armed: reconstructing correct
/// objects with the wrong matrix would corrupt data the test never meant to
/// touch, and would prove nothing about encoding.
///
/// # Errors
///
/// Returns [`CodecError::EngineUnavailable`] when test hooks are not enabled
/// for this process. Arming is refused rather than ignored, so a caller cannot
/// believe the hook is on when it is not.
pub fn set_foreign_matrix(enabled: bool) -> Result<(), CodecError> {
    if enabled && !hooks_enabled() {
        return Err(CodecError::EngineUnavailable {
            reason: format!("EC test hooks are disabled; set {TEST_HOOKS_ENV}=true to arm them"),
        });
    }
    FOREIGN_MATRIX.store(enabled, Ordering::Relaxed);
    Ok(())
}

/// Whether the accelerated engine should encode with the wrong matrix.
///
/// Called once per encoded chunk, so it must stay cheap: the environment is
/// read once for the life of the process and only the atomic is touched here.
#[must_use]
pub fn foreign_matrix_armed() -> bool {
    FOREIGN_MATRIX.load(Ordering::Relaxed) && hooks_enabled()
}

/// Whether EC test hooks are enabled for this process.
///
/// This is the only definition of that question for the codec, so a hook cannot
/// pass one gate and be refused by another.
#[must_use]
pub fn hooks_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| truthy(std::env::var(TEST_HOOKS_ENV)))
}

/// Environment flag that makes this process recover LRC chunks the way an EC v1
/// node did: the two fast paths, and nothing behind them.
pub const LRC_FAST_PATH_ONLY_ENV: &str = "S4_EC_TEST_LRC_FAST_PATH_ONLY";

/// Whether LRC recovery on this process stops where the fast paths stop.
///
/// This is how a cluster built from one binary runs half upgraded. A node
/// started with the flag decodes, repairs and documents exactly what EC v1
/// reached, so the acceptance script can put a solver node and a pre-solver
/// node over the same shards and watch what each of them makes of them.
///
/// Encoding is untouched: the flag removes a recovery path and nothing else,
/// and both editions write the same bytes (decision EC2-103, section 7). Read
/// once and never re-read, because a node does not change edition while it
/// runs.
#[must_use]
pub fn lrc_fast_path_only() -> bool {
    static FAST_PATH_ONLY: OnceLock<bool> = OnceLock::new();

    *FAST_PATH_ONLY.get_or_init(|| hooks_enabled() && truthy(std::env::var(LRC_FAST_PATH_ONLY_ENV)))
}

/// Whether an environment variable holds one of the values S4 reads as "on".
///
/// An unset variable, an unreadable one and a value nobody meant as "yes" all
/// leave the hook disarmed: test scaffolding has to be asked for explicitly.
fn truthy(value: Result<String, std::env::VarError>) -> bool {
    value.is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// A valid generator matrix that is not the one S4 stores data with.
///
/// # Errors
///
/// Returns [`CodecError::Arithmetic`] when the dimensions are unusable.
pub fn foreign_generator_matrix(k: u8, total_shards: u8) -> Result<Vec<Vec<u8>>, CodecError> {
    systematic_matrix_from_point(k, total_shards, FOREIGN_VANDERMONDE_POINT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::systematic_generator_matrix;
    use crate::types::EcProfileSnapshot;

    /// The hook is only useful if its matrix really differs — on every profile.
    #[test]
    fn foreign_matrix_differs_from_the_stored_format() {
        for profile in [
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let total = profile.total_shards();
            let real = systematic_generator_matrix(profile.k, total).expect("valid dimensions");
            let foreign = foreign_generator_matrix(profile.k, total).expect("valid dimensions");

            assert_ne!(
                real, foreign,
                "profile {} would not notice the foreign matrix",
                profile.name
            );
        }
    }

    /// Data shards must still pass through untouched, or the difference the
    /// script sees would be an unrelated failure rather than wrong parity.
    #[test]
    fn foreign_matrix_is_still_systematic() {
        let profile = EcProfileSnapshot::rs_standard();
        let foreign = foreign_generator_matrix(profile.k, profile.total_shards()).unwrap();

        for (row, coefficients) in foreign.iter().take(usize::from(profile.k)).enumerate() {
            let expected = (0..usize::from(profile.k))
                .map(|column| u8::from(column == row))
                .collect::<Vec<_>>();
            assert_eq!(*coefficients, expected, "row {row} is not an identity row");
        }
    }

    /// Only the values an operator would write as "yes" arm anything, and a
    /// variable nobody set arms nothing.
    #[test]
    fn only_a_deliberate_yes_is_read_as_on() {
        for value in ["1", "true", "TRUE", " yes ", "on"] {
            assert!(truthy(Ok(value.to_string())), "{value:?} should read as on");
        }
        for value in ["", "0", "false", "no", "off", "maybe"] {
            assert!(
                !truthy(Ok(value.to_string())),
                "{value:?} should read as off"
            );
        }
        assert!(!truthy(Err(std::env::VarError::NotPresent)));
        assert!(!truthy(Err(std::env::VarError::NotUnicode(
            "\u{fffd}".into()
        ))));
    }

    /// The flag alone must not arm the edition hook: an acceptance run exports
    /// `S4_EC_TEST_HOOKS_ENABLED` for the whole suite, and every node in it
    /// would otherwise lose the solver.
    #[test]
    fn the_edition_hook_needs_its_own_variable() {
        if std::env::var(LRC_FAST_PATH_ONLY_ENV).is_ok() {
            return;
        }
        assert!(!lrc_fast_path_only());
    }

    /// Arming without the environment flag must fail loudly, not quietly.
    #[test]
    fn arming_is_refused_while_hooks_are_disabled() {
        // Tests share a process, so the flag is left as the environment has it;
        // this only asserts the branch that applies when it is absent.
        if std::env::var(TEST_HOOKS_ENV).is_ok() {
            return;
        }
        assert!(set_foreign_matrix(true).is_err());
        assert!(!foreign_matrix_armed());
        // Disarming always works, so a cleanup path can never get stuck.
        assert!(set_foreign_matrix(false).is_ok());
    }
}
