// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Picks the codec engine this node runs on and builds codecs with it.
//!
//! `s4-codec` owns the rule (`select_engine`) but cannot apply it: the ISA-L
//! backend depends on `s4-codec`, so only a layer above both can hold the
//! optional dependency. That layer is this module, and it is the single place
//! the rest of the EC runtime builds codecs through.
//!
//! The engine is resolved once per process and never changes afterwards.
//! Changing it means restarting the node, which is safe at any moment: both
//! engines produce identical bytes, so nothing has to be migrated or
//! re-encoded. See decisions EC2-002 and EC2-004.

use s4_codec::{
    select_engine, CodecEngine, CodecEngineKind, CodecEngineSelection, CodecEngineSetting,
    CodecError, EcCodecKind, EcProfileSnapshot, EngineAvailability,
};
use std::sync::OnceLock;

/// True when an accelerated backend is linked in and usable on this target.
///
/// The feature alone is not enough: off x86_64 the ISA-L crate compiles to
/// nothing, so a build can carry the feature and still have no codec.
const ISAL_BUILT: bool = cfg!(all(feature = "isal", target_arch = "x86_64"));

/// Build a codec for a validated profile, on the engine this node selected.
///
/// Reed-Solomon profiles may go to either engine; both produce the same bytes,
/// so callers do not care which one they get. LRC profiles always use the
/// portable engine — their local parity is plain XOR, which already runs at
/// memory speed (decision EC2-005).
///
/// # Errors
///
/// Returns [`CodecError`] when the profile cannot build a codec.
pub fn build_codec(profile: EcProfileSnapshot) -> Result<Box<dyn CodecEngine>, CodecError> {
    build_with(profile, MatrixSource::Stored)
}

/// Build a codec for writing new shards.
///
/// Identical to [`build_codec`] except that the foreign-matrix test hook, when
/// armed, applies here and only here. Encoding with the wrong matrix is the
/// mistake the hook exists to make; decoding or repairing with it would instead
/// corrupt objects that were written correctly, which proves nothing and is not
/// what the acceptance script asks for.
///
/// # Errors
///
/// Returns [`CodecError`] when the profile cannot build a codec.
pub fn build_encoding_codec(
    profile: EcProfileSnapshot,
) -> Result<Box<dyn CodecEngine>, CodecError> {
    build_with(profile, MatrixSource::TestHookAware)
}

/// Which generator matrix a codec should be built on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatrixSource {
    /// The matrix that defines the stored format. The only choice in production.
    Stored,
    /// The stored matrix, unless the foreign-matrix test hook is armed.
    TestHookAware,
}

fn build_with(
    profile: EcProfileSnapshot,
    matrix: MatrixSource,
) -> Result<Box<dyn CodecEngine>, CodecError> {
    if profile.codec == EcCodecKind::ReedSolomon && codec_engine() == CodecEngineKind::Isal {
        return build_accelerated(profile, matrix);
    }
    s4_codec::build_codec(profile)
}

/// Engine used by this process.
///
/// When the operator asked for something impossible this reports the portable
/// engine, because that is what the codec would fall back to. Refusing to start
/// is [`ensure_codec_engine_available`]'s job, and the EC runtime calls it
/// before any of this matters.
#[must_use]
pub fn codec_engine() -> CodecEngineKind {
    selection().as_ref().map_or(CodecEngineKind::Pure, |selection| selection.engine)
}

/// Fails when the operator asked for an engine this process cannot provide.
///
/// Called while the EC runtime opens, so a misconfigured node stops with a
/// clear message instead of quietly running slower than it was told to.
///
/// # Errors
///
/// Returns [`CodecError::EngineUnavailable`] with the reason the request could
/// not be met.
pub fn ensure_codec_engine_available() -> Result<(), CodecError> {
    match selection() {
        Ok(_) => Ok(()),
        Err(reason) => Err(CodecError::EngineUnavailable {
            reason: reason.clone(),
        }),
    }
}

/// Selection for this process, resolved once and logged once.
fn selection() -> &'static Result<CodecEngineSelection, String> {
    static SELECTION: OnceLock<Result<CodecEngineSelection, String>> = OnceLock::new();

    SELECTION.get_or_init(|| {
        let availability = EngineAvailability {
            isal_built: ISAL_BUILT,
            cpu_supported: s4_codec::cpu::supports_isal_acceleration(),
        };
        let resolved = CodecEngineSetting::from_env()
            .and_then(|setting| select_engine(setting, availability))
            .map_err(|error| error.to_string());

        match &resolved {
            Ok(selection) => tracing::info!(
                engine = selection.engine.as_str(),
                reason = selection.reason,
                isal_built = availability.isal_built,
                cpu_supported = availability.cpu_supported,
                "EC codec engine selected"
            ),
            Err(reason) => tracing::error!(reason, "EC codec engine is unavailable"),
        }
        resolved
    })
}

#[cfg(all(feature = "isal", target_arch = "x86_64"))]
fn build_accelerated(
    profile: EcProfileSnapshot,
    matrix: MatrixSource,
) -> Result<Box<dyn CodecEngine>, CodecError> {
    use s4_codec_isal::IsalReedSolomonCodec;

    if matrix == MatrixSource::TestHookAware && s4_codec::test_hooks::foreign_matrix_armed() {
        // Only the acceptance script gets here, and only to prove that encoding
        // with the wrong matrix is detectable. See `s4_codec::test_hooks`.
        let generator =
            s4_codec::test_hooks::foreign_generator_matrix(profile.k, profile.total_shards())?;
        return Ok(Box::new(IsalReedSolomonCodec::with_generator_matrix(
            profile, generator,
        )?));
    }
    Ok(Box::new(IsalReedSolomonCodec::new(profile)?))
}

/// Unreachable without the feature: the engine rule cannot select ISA-L when
/// `ISAL_BUILT` is false. Present so the caller compiles either way.
#[cfg(not(all(feature = "isal", target_arch = "x86_64")))]
fn build_accelerated(
    profile: EcProfileSnapshot,
    _matrix: MatrixSource,
) -> Result<Box<dyn CodecEngine>, CodecError> {
    s4_codec::build_codec(profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whatever this build is, the process must resolve to a usable engine and
    /// keep answering the same way.
    #[test]
    fn process_engine_is_resolvable_and_stable() {
        let engine = codec_engine();
        assert_eq!(engine, codec_engine());

        if !ISAL_BUILT {
            assert_eq!(
                engine,
                CodecEngineKind::Pure,
                "without the isal feature there is nothing else to select"
            );
        } else if s4_codec::cpu::supports_isal_acceleration() {
            // The default is `auto`, and this build and machine can both offer
            // acceleration, so anything else means the wiring is broken.
            assert_eq!(
                engine,
                CodecEngineKind::Isal,
                "an isal build on a capable CPU must select the accelerated engine"
            );
        }
    }

    /// The factory must serve every standard profile on whichever engine this
    /// build selected, and the codec it returns must describe that profile.
    #[test]
    fn factory_builds_every_standard_profile() {
        for profile in [
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let codec = build_codec(profile.clone()).expect("standard profile builds");
            assert_eq!(codec.profile().name, profile.name);
        }
    }

    /// Whichever engine is selected, its output has to be readable by the
    /// portable one. This is the mixed cluster reduced to a single process.
    #[test]
    fn shards_written_here_are_readable_by_the_portable_engine() {
        let profile = EcProfileSnapshot::rs_standard();
        let payload = (0..40_000u32).map(|byte| (byte % 251) as u8).collect::<Vec<u8>>();

        let selected = build_codec(profile.clone()).expect("selected engine builds");
        let portable = s4_codec::build_codec(profile).expect("portable engine builds");

        let encoded = selected.encode_chunk(&payload, 0).expect("encode");
        let shards = encoded.shards.iter().cloned().map(Some).collect::<Vec<_>>();
        let decoded = portable.decode_chunk(&shards, encoded.original_size).expect("decode");

        assert_eq!(decoded.as_ref(), payload.as_slice());
        assert_eq!(
            encoded,
            portable.encode_chunk(&payload, 0).expect("portable encode"),
            "the selected engine must produce the same shards as the portable one"
        );
    }
}
