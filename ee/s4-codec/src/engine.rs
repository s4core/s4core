// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! The rule that decides which implementation computes the codec arithmetic.
//!
//! Both engines produce identical bytes (decision EC2-002), so the choice is
//! about speed alone: a node can switch engines on restart, and a cluster can
//! run a mix of them indefinitely without any action on the stored data.
//!
//! Only the rule lives here, as a pure function, so every row of the matrix in
//! decision EC2-004 can be tested without touching the environment or the
//! machine. Applying it — caching the answer, logging it once, and building the
//! accelerated codec — belongs to the EC runtime, which is the layer that knows
//! whether the ISA-L crate was linked in.

use serde::Serialize;

use crate::types::CodecError;

/// Environment variable that overrides the automatic choice.
pub const CODEC_ENGINE_ENV: &str = "S4_EC_CODEC_ENGINE";

/// Implementation that performs the codec arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodecEngineKind {
    /// Portable Rust implementation. Always available.
    Pure,
    /// SIMD implementation backed by Intel ISA-L.
    Isal,
}

impl CodecEngineKind {
    /// Stable name used in logs, admin status and acceptance tests.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pure => "pure",
            Self::Isal => "isal",
        }
    }
}

/// What the operator asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CodecEngineSetting {
    /// Take the fastest engine available, never fail.
    #[default]
    Auto,
    /// Insist on ISA-L; refuse to start if it cannot be provided.
    Isal,
    /// Insist on the portable engine.
    Pure,
}

impl CodecEngineSetting {
    /// Parse the value of [`CODEC_ENGINE_ENV`].
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::EngineUnavailable`] for anything but `auto`,
    /// `isal` or `pure`. A typo must not be read as "auto": an operator who
    /// wrote `isa-l` wants acceleration and needs to be told, not defaulted.
    pub fn parse(value: &str) -> Result<Self, CodecError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Self::Auto),
            "isal" => Ok(Self::Isal),
            "pure" => Ok(Self::Pure),
            other => Err(CodecError::EngineUnavailable {
                reason: format!(
                    "{CODEC_ENGINE_ENV} must be 'auto', 'isal' or 'pure', got '{other}'"
                ),
            }),
        }
    }

    /// Read the setting from the environment, defaulting to [`Self::Auto`].
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::EngineUnavailable`] when the variable holds an
    /// unknown value, including one that is not valid UTF-8. Only an unset
    /// variable means "auto": a value that could not be read is a value the
    /// operator meant, and treating it as absent is how a node ends up on the
    /// portable engine without anyone being told.
    pub fn from_env() -> Result<Self, CodecError> {
        Self::from_env_result(std::env::var(CODEC_ENGINE_ENV))
    }

    /// The decision [`Self::from_env`] makes, separated from reading the
    /// environment so it can be tested without mutating the process.
    fn from_env_result(value: Result<String, std::env::VarError>) -> Result<Self, CodecError> {
        match value {
            Ok(value) => Self::parse(&value),
            Err(std::env::VarError::NotPresent) => Ok(Self::Auto),
            Err(std::env::VarError::NotUnicode(value)) => Err(CodecError::EngineUnavailable {
                reason: format!("{CODEC_ENGINE_ENV} is not valid UTF-8: {value:?}"),
            }),
        }
    }
}

/// What this build and this machine can actually offer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineAvailability {
    /// The accelerated backend is linked in and usable on this target.
    ///
    /// The build feature alone is not enough: off x86_64 the ISA-L crate
    /// compiles to nothing, so a build can carry the feature and still have no
    /// accelerated codec.
    pub isal_built: bool,
    /// The CPU has the instructions that make ISA-L worthwhile.
    pub cpu_supported: bool,
}

/// The engine to use, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecEngineSelection {
    /// Engine that will do the arithmetic.
    pub engine: CodecEngineKind,
    /// Short explanation, written for a log line an operator will read.
    pub reason: &'static str,
}

/// Decides which engine to use, or refuses when the request cannot be met.
///
/// `auto` never fails: it is the default and has to work on a laptop without
/// AVX2 and on an ARM build alike. An explicit `isal` does fail, because an
/// operator who asked for acceleration and silently got the portable engine
/// would only find out through a puzzling difference in throughput between
/// nodes. See decision EC2-004.
///
/// # Errors
///
/// Returns [`CodecError::EngineUnavailable`] when `isal` was requested and this
/// build or CPU cannot provide it.
pub fn select_engine(
    setting: CodecEngineSetting,
    availability: EngineAvailability,
) -> Result<CodecEngineSelection, CodecError> {
    match setting {
        CodecEngineSetting::Pure => Ok(CodecEngineSelection {
            engine: CodecEngineKind::Pure,
            reason: "the portable engine was requested",
        }),
        CodecEngineSetting::Isal => {
            if !availability.isal_built {
                return Err(CodecError::EngineUnavailable {
                    reason: format!(
                        "{CODEC_ENGINE_ENV}=isal was requested but this binary was built without \
                         the isal feature; rebuild with --features isal or use 'auto'"
                    ),
                });
            }
            if !availability.cpu_supported {
                return Err(CodecError::EngineUnavailable {
                    reason: format!(
                        "{CODEC_ENGINE_ENV}=isal was requested but this CPU does not support AVX2; \
                         use 'auto' to fall back to the portable engine"
                    ),
                });
            }
            Ok(CodecEngineSelection {
                engine: CodecEngineKind::Isal,
                reason: "the accelerated engine was requested",
            })
        }
        CodecEngineSetting::Auto => {
            if !availability.isal_built {
                return Ok(CodecEngineSelection {
                    engine: CodecEngineKind::Pure,
                    reason: "the isal feature is not built",
                });
            }
            if !availability.cpu_supported {
                return Ok(CodecEngineSelection {
                    engine: CodecEngineKind::Pure,
                    reason: "this CPU does not support AVX2",
                });
            }
            Ok(CodecEngineSelection {
                engine: CodecEngineKind::Isal,
                reason: "the accelerated engine is available",
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOTHING: EngineAvailability = EngineAvailability {
        isal_built: false,
        cpu_supported: false,
    };
    const BUILT_ONLY: EngineAvailability = EngineAvailability {
        isal_built: true,
        cpu_supported: false,
    };
    const EVERYTHING: EngineAvailability = EngineAvailability {
        isal_built: true,
        cpu_supported: true,
    };

    fn engine(
        setting: CodecEngineSetting,
        availability: EngineAvailability,
    ) -> Result<CodecEngineKind, CodecError> {
        select_engine(setting, availability).map(|selection| selection.engine)
    }

    /// Every row of the matrix in decision EC2-004.
    ///
    /// A build without the feature and a target that cannot use it are the same
    /// case here, which is why `isal_built` already folds the architecture in.
    #[test]
    fn selection_matches_the_decision_matrix() {
        // auto never fails, whatever is available.
        assert_eq!(
            engine(CodecEngineSetting::Auto, EVERYTHING).unwrap(),
            CodecEngineKind::Isal
        );
        assert_eq!(
            engine(CodecEngineSetting::Auto, BUILT_ONLY).unwrap(),
            CodecEngineKind::Pure
        );
        assert_eq!(
            engine(CodecEngineSetting::Auto, NOTHING).unwrap(),
            CodecEngineKind::Pure
        );

        // An explicit request is honoured when it can be.
        assert_eq!(
            engine(CodecEngineSetting::Isal, EVERYTHING).unwrap(),
            CodecEngineKind::Isal
        );

        // ...and refused when it cannot, rather than silently downgraded.
        assert!(engine(CodecEngineSetting::Isal, BUILT_ONLY).is_err());
        assert!(engine(CodecEngineSetting::Isal, NOTHING).is_err());

        // pure is always available.
        for availability in [NOTHING, BUILT_ONLY, EVERYTHING] {
            assert_eq!(
                engine(CodecEngineSetting::Pure, availability).unwrap(),
                CodecEngineKind::Pure
            );
        }
    }

    /// The refusal has to say which of the two conditions failed.
    #[test]
    fn refusal_names_the_missing_condition() {
        let missing_feature =
            select_engine(CodecEngineSetting::Isal, NOTHING).unwrap_err().to_string();
        assert!(
            missing_feature.contains("isal feature"),
            "{missing_feature}"
        );

        let missing_cpu =
            select_engine(CodecEngineSetting::Isal, BUILT_ONLY).unwrap_err().to_string();
        assert!(missing_cpu.contains("AVX2"), "{missing_cpu}");
    }

    #[test]
    fn setting_parses_accepted_values_and_rejects_typos() {
        assert_eq!(
            CodecEngineSetting::parse("").unwrap(),
            CodecEngineSetting::Auto
        );
        assert_eq!(
            CodecEngineSetting::parse(" AUTO ").unwrap(),
            CodecEngineSetting::Auto
        );
        assert_eq!(
            CodecEngineSetting::parse("isal").unwrap(),
            CodecEngineSetting::Isal
        );
        assert_eq!(
            CodecEngineSetting::parse("Pure").unwrap(),
            CodecEngineSetting::Pure
        );
        assert!(CodecEngineSetting::parse("isa-l").is_err());
    }

    /// An unreadable value must not quietly become the default.
    #[test]
    fn non_unicode_setting_is_an_error_not_auto() {
        use std::env::VarError;
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let broken = OsString::from_vec(vec![0x69, 0x73, 0xff, 0x61, 0x6c]);
        let result = CodecEngineSetting::from_env_result(Err(VarError::NotUnicode(broken)));
        assert!(result.is_err(), "a non-UTF-8 setting must be refused");

        // An unset variable is the one case that legitimately means "auto".
        assert_eq!(
            CodecEngineSetting::from_env_result(Err(VarError::NotPresent)).unwrap(),
            CodecEngineSetting::Auto
        );
    }

    #[test]
    fn engine_names_are_the_ones_admin_status_publishes() {
        assert_eq!(CodecEngineKind::Pure.as_str(), "pure");
        assert_eq!(CodecEngineKind::Isal.as_str(), "isal");
    }
}
