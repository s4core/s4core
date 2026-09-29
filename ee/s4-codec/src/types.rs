// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Public codec types shared by the Reed-Solomon and LRC engines.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{lrc::LrcCodec, rs::ReedSolomonCodec};

/// Default minimum object size for EC transcode eligibility: 1 MiB.
pub const DEFAULT_MIN_OBJECT_SIZE: u64 = 1_048_576;
/// Default logical chunk size before shard encoding: 6 MiB.
pub const DEFAULT_CHUNK_SIZE: u64 = 6_291_456;

/// Erasure coding family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcCodecKind {
    /// MDS Reed-Solomon profile.
    ReedSolomon,
    /// Locally repairable code profile.
    LocallyRepairable,
}

/// Parity layout for an EC profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcParity {
    /// Reed-Solomon parity shards.
    ReedSolomon {
        /// Number of parity shards.
        m: u8,
    },
    /// LRC local and global parity shards.
    Lrc {
        /// Number of local parity groups.
        local: u8,
        /// Number of global parity shards.
        global: u8,
    },
}

impl EcParity {
    pub(crate) fn total_parity(self) -> u16 {
        match self {
            Self::ReedSolomon { m } => u16::from(m),
            Self::Lrc { local, global } => u16::from(local) + u16::from(global),
        }
    }
}

/// Immutable snapshot of an EC profile used by codec engines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcProfileSnapshot {
    /// Stable profile name.
    pub name: String,
    /// Codec family.
    pub codec: EcCodecKind,
    /// Number of data shards.
    pub k: u8,
    /// Parity layout.
    pub parity: EcParity,
    /// Objects below this size remain explicitly replicated in the EC pool.
    pub min_object_size: u64,
    /// Logical chunk size before shard encoding.
    pub chunk_size: u64,
}

impl EcProfileSnapshot {
    /// `EC-RS-Small`: RS(4,2), 6 total shards.
    pub fn rs_small() -> Self {
        Self::new_rs_unchecked("ec-rs-small", 4, 2)
    }

    /// `EC-RS-Standard`: RS(6,3), 9 total shards.
    pub fn rs_standard() -> Self {
        Self::new_rs_unchecked("ec-rs-standard", 6, 3)
    }

    /// `EC-RS-Dense`: RS(8,3), 11 total shards.
    pub fn rs_dense() -> Self {
        Self::new_rs_unchecked("ec-rs-dense", 8, 3)
    }

    /// `EC-LRC-Small`: LRC(4,2,1), 7 total shards.
    pub fn lrc_small() -> Self {
        Self::new_lrc_unchecked("ec-lrc-small", 4, 2, 1)
    }

    /// `EC-LRC-Standard`: LRC(6,2,2), 10 total shards.
    pub fn lrc_standard() -> Self {
        Self::new_lrc_unchecked("ec-lrc-standard", 6, 2, 2)
    }

    /// `EC-LRC-Dense`: LRC(10,2,2), 14 total shards.
    pub fn lrc_dense() -> Self {
        Self::new_lrc_unchecked("ec-lrc-dense", 10, 2, 2)
    }

    /// Build and validate a Reed-Solomon profile snapshot.
    pub fn new_rs(name: impl Into<String>, k: u8, m: u8) -> Result<Self, CodecError> {
        let profile = Self::new_rs_unchecked(name, k, m);
        profile.validate()?;
        Ok(profile)
    }

    /// Build and validate an LRC profile snapshot.
    pub fn new_lrc(
        name: impl Into<String>,
        k: u8,
        local: u8,
        global: u8,
    ) -> Result<Self, CodecError> {
        let profile = Self::new_lrc_unchecked(name, k, local, global);
        profile.validate()?;
        Ok(profile)
    }

    fn new_rs_unchecked(name: impl Into<String>, k: u8, m: u8) -> Self {
        Self {
            name: name.into(),
            codec: EcCodecKind::ReedSolomon,
            k,
            parity: EcParity::ReedSolomon { m },
            min_object_size: DEFAULT_MIN_OBJECT_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    fn new_lrc_unchecked(name: impl Into<String>, k: u8, local: u8, global: u8) -> Self {
        Self {
            name: name.into(),
            codec: EcCodecKind::LocallyRepairable,
            k,
            parity: EcParity::Lrc { local, global },
            min_object_size: DEFAULT_MIN_OBJECT_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Validate the profile in isolation.
    pub fn validate(&self) -> Result<(), CodecError> {
        if self.name.trim().is_empty() {
            return Err(CodecError::InvalidProfile {
                profile: self.name.clone(),
                reason: "name must not be empty".to_string(),
            });
        }
        if self.k == 0 {
            return Err(CodecError::InvalidProfile {
                profile: self.name.clone(),
                reason: "k must be at least 1".to_string(),
            });
        }
        if self.min_object_size == 0 {
            return Err(CodecError::InvalidProfile {
                profile: self.name.clone(),
                reason: "min_object_size must be greater than 0".to_string(),
            });
        }
        if self.chunk_size == 0 {
            return Err(CodecError::InvalidProfile {
                profile: self.name.clone(),
                reason: "chunk_size must be greater than 0".to_string(),
            });
        }
        match (self.codec, self.parity) {
            (EcCodecKind::ReedSolomon, EcParity::ReedSolomon { m }) => {
                if m == 0 {
                    return Err(CodecError::InvalidProfile {
                        profile: self.name.clone(),
                        reason: "Reed-Solomon parity m must be at least 1".to_string(),
                    });
                }
            }
            (EcCodecKind::LocallyRepairable, EcParity::Lrc { local, global }) => {
                if local == 0 {
                    return Err(CodecError::InvalidProfile {
                        profile: self.name.clone(),
                        reason: "LRC local parity count must be at least 1".to_string(),
                    });
                }
                if global == 0 {
                    return Err(CodecError::InvalidProfile {
                        profile: self.name.clone(),
                        reason: "LRC global parity count must be at least 1".to_string(),
                    });
                }
                if local > self.k {
                    return Err(CodecError::InvalidProfile {
                        profile: self.name.clone(),
                        reason: "LRC local parity count must not exceed k".to_string(),
                    });
                }
                if !self.k.is_multiple_of(local) {
                    return Err(CodecError::InvalidProfile {
                        profile: self.name.clone(),
                        reason: "LRC v1 requires k to be divisible by local parity count"
                            .to_string(),
                    });
                }
            }
            (EcCodecKind::ReedSolomon, EcParity::Lrc { .. }) => {
                return Err(CodecError::InvalidProfile {
                    profile: self.name.clone(),
                    reason: "Reed-Solomon profiles must use ReedSolomon parity".to_string(),
                });
            }
            (EcCodecKind::LocallyRepairable, EcParity::ReedSolomon { .. }) => {
                return Err(CodecError::InvalidProfile {
                    profile: self.name.clone(),
                    reason: "LRC profiles must use Lrc parity".to_string(),
                });
            }
        }

        let total = u16::from(self.k) + self.parity.total_parity();
        if total > u16::from(u8::MAX) {
            return Err(CodecError::InvalidProfile {
                profile: self.name.clone(),
                reason: format!("total shard count must fit in u8, got {total}"),
            });
        }
        Ok(())
    }

    /// Total number of shard slots in this profile.
    pub fn total_shards(&self) -> u8 {
        let total = u16::from(self.k) + self.parity.total_parity();
        u8::try_from(total).unwrap_or(u8::MAX)
    }

    /// Quorum used for EC manifest commit/tombstones.
    pub fn manifest_quorum(&self) -> u8 {
        (self.total_shards() / 2) + 1
    }

    /// Inline EC write requires every shard to be durable in v1.
    pub fn required_shard_acks_inline(&self) -> u8 {
        self.total_shards()
    }

    /// Minimum normal-read shard count.
    pub fn read_minimum_normal(&self) -> u8 {
        self.k
    }

    /// Role expected for a slot in this profile.
    pub fn role_for_slot(&self, slot_index: u8) -> Result<ShardRole, CodecError> {
        if slot_index >= self.total_shards() {
            return Err(CodecError::InvalidShardSet {
                reason: format!(
                    "slot index {slot_index} is outside profile '{}' with {} shard(s)",
                    self.name,
                    self.total_shards()
                ),
            });
        }
        match self.parity {
            EcParity::ReedSolomon { .. } => {
                if slot_index < self.k {
                    Ok(ShardRole::Data)
                } else {
                    Ok(ShardRole::ReedSolomonParity)
                }
            }
            EcParity::Lrc { local, .. } => {
                if slot_index < self.k {
                    Ok(ShardRole::Data)
                } else if slot_index < self.k + local {
                    Ok(ShardRole::LrcLocalParity)
                } else {
                    Ok(ShardRole::LrcGlobalParity)
                }
            }
        }
    }
}

/// Role of a shard produced by a codec engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardRole {
    /// Data shard.
    Data,
    /// Reed-Solomon parity shard.
    ReedSolomonParity,
    /// LRC local parity shard.
    LrcLocalParity,
    /// LRC global parity shard.
    LrcGlobalParity,
}

/// One verified shard candidate used for decode and repair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shard {
    /// Zero-based slot index in the erasure set.
    pub slot_index: u8,
    /// Logical chunk index in the object.
    pub chunk_index: u32,
    /// Shard role for this slot.
    pub role: ShardRole,
    /// Shard bytes.
    pub data: Bytes,
    /// CRC32 of `data`.
    pub crc32: u32,
    /// SHA-256 of `data`.
    pub sha256: [u8; 32],
    /// Byte length of `data`.
    pub size: u64,
}

impl Shard {
    /// Build a shard and compute its integrity fields.
    pub fn new(
        slot_index: u8,
        chunk_index: u32,
        role: ShardRole,
        data: Bytes,
    ) -> Result<Self, CodecError> {
        let size = u64::try_from(data.len()).map_err(|_| CodecError::InvalidInput {
            reason: "shard size does not fit in u64".to_string(),
        })?;
        let crc32 = crc32fast::hash(&data);
        let sha256 = sha256(&data);
        Ok(Self {
            slot_index,
            chunk_index,
            role,
            data,
            crc32,
            sha256,
            size,
        })
    }

    /// Recompute and verify integrity fields stored with this shard.
    pub fn verify_integrity(&self) -> Result<(), CodecError> {
        let actual_size =
            u64::try_from(self.data.len()).map_err(|_| CodecError::InvalidShardSet {
                reason: "shard size does not fit in u64".to_string(),
            })?;
        if self.size != actual_size {
            return Err(CodecError::InvalidShardSet {
                reason: format!(
                    "slot {} declares size {} but contains {} byte(s)",
                    self.slot_index, self.size, actual_size
                ),
            });
        }
        let actual_crc32 = crc32fast::hash(&self.data);
        if self.crc32 != actual_crc32 {
            return Err(CodecError::HashMismatch {
                slot_index: self.slot_index,
                kind: "crc32",
            });
        }
        let actual_sha256 = sha256(&self.data);
        if self.sha256 != actual_sha256 {
            return Err(CodecError::HashMismatch {
                slot_index: self.slot_index,
                kind: "sha256",
            });
        }
        Ok(())
    }
}

/// Encoded shards for one logical input chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodedChunk {
    /// Logical chunk index in the object.
    pub chunk_index: u32,
    /// Unencoded byte length of this chunk.
    pub original_size: u64,
    /// Byte length of every encoded shard.
    pub shard_size: u64,
    /// Shards produced by the codec.
    pub shards: Vec<Shard>,
}

/// How a codec rebuilds the data of one documented failure pattern.
///
/// Always computed from the missing slots, never read off the entry's label.
/// The difference is visible in `ec-lrc-small`, where the entry named after the
/// global Reed-Solomon budget is a single lost data shard that local XOR
/// repairs from two reads: an operator asking this field is asking what the
/// degraded read will cost, not what the pattern is called.
///
/// See decision EC2-103.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPath {
    /// Every data shard is present; there is nothing to rebuild.
    Direct,
    /// XOR inside one LRC local group: `k / local` reads and no cross-zone
    /// traffic.
    LocalXor,
    /// The global Reed-Solomon stage, which reads `k` shards.
    GlobalRs,
    /// Only the unified local-plus-global solver reaches this pattern.
    UnifiedSolver,
    /// Nothing rebuilds this pattern.
    None,
}

impl RecoveryPath {
    /// Whether a pattern taking this path is recovered at all.
    #[must_use]
    pub fn recovers(self) -> bool {
        self != Self::None
    }

    /// Stable name, the one the JSON document carries. Used by log fields and
    /// metric labels so that a dashboard and the failure matrix spell a path
    /// the same way.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::LocalXor => "local_xor",
            Self::GlobalRs => "global_rs",
            Self::UnifiedSolver => "unified_solver",
            Self::None => "none",
        }
    }
}

/// One documented failure pattern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureMatrixEntry {
    /// Stable pattern label.
    pub label: String,
    /// Missing shard slots for this pattern.
    pub missing_slots: Vec<u8>,
    /// Whether the profile can recover this pattern.
    pub recoverable: bool,
    /// Which path recovers this pattern, and at what cost.
    pub recovery_path: RecoveryPath,
    /// Human-readable explanation.
    pub reason: String,
}

/// Documented codec failure matrix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureMatrix {
    /// Which set of patterns this table documents. See
    /// [`FailureMatrix::VERSION_FAST_PATH`].
    pub version: u32,
    /// Profile name.
    pub profile_name: String,
    /// Codec family.
    pub codec: EcCodecKind,
    /// Number of data shards.
    pub k: u8,
    /// Total shard slots.
    pub total_shards: u8,
    /// Documented failure patterns.
    pub entries: Vec<FailureMatrixEntry>,
}

impl FailureMatrix {
    /// A table of what the fast recovery paths alone reach — the EC v1
    /// guarantees.
    pub const VERSION_FAST_PATH: u32 = 1;

    /// A table that also documents the patterns only the unified solver
    /// reaches.
    ///
    /// Version 1 is the version 2 table with every
    /// [`RecoveryPath::UnifiedSolver`] entry dropped: the earlier entries keep
    /// their labels, slots, verdicts and order, and the new ones are appended.
    /// Tables of different versions therefore differ by an addition rather than
    /// by a disagreement — which matters while a cluster is half upgraded, as
    /// operators compare this document between nodes byte for byte.
    pub const VERSION_UNIFIED_SOLVER: u32 = 2;

    /// Documents `entries` for `profile`, numbering the table by what its
    /// entries rest on.
    ///
    /// The version describes the contents, not the build or the JSON schema: a
    /// table becomes version 2 the moment it documents a pattern that only the
    /// unified solver recovers, and a profile that gains nothing from the
    /// solver keeps version 1 and says so honestly. Every engine builds its
    /// document through here, so no table can be numbered by hand.
    #[must_use]
    pub fn new(profile: &EcProfileSnapshot, entries: Vec<FailureMatrixEntry>) -> Self {
        let solved = entries.iter().any(|entry| entry.recovery_path == RecoveryPath::UnifiedSolver);
        Self {
            version: if solved {
                Self::VERSION_UNIFIED_SOLVER
            } else {
                Self::VERSION_FAST_PATH
            },
            profile_name: profile.name.clone(),
            codec: profile.codec,
            k: profile.k,
            total_shards: profile.total_shards(),
            entries,
        }
    }
}

/// Runtime-independent codec engine interface.
pub trait CodecEngine: Send + Sync {
    /// Profile snapshot used by this engine.
    fn profile(&self) -> &EcProfileSnapshot;

    /// Encode one logical chunk.
    fn encode_chunk(&self, input: &[u8], chunk_index: u32) -> Result<EncodedChunk, CodecError>;

    /// Decode one logical chunk from verified shard candidates.
    fn decode_chunk(
        &self,
        shards: &[Option<Shard>],
        original_size: u64,
    ) -> Result<Bytes, CodecError>;

    /// Repair one missing shard from verified shard candidates.
    fn repair_shard(&self, shards: &[Option<Shard>], missing_slot: u8)
        -> Result<Shard, CodecError>;

    /// Which path [`Self::decode_chunk`] takes once the slots marked in `lost`
    /// are gone.
    ///
    /// `lost[slot]` is true when that shard is gone, and a slot past the end of
    /// `lost` counts as present — the convention [`crate::LossTolerance`] asks
    /// in. Answered without shards, so a caller can ask before reading anything
    /// and the read and repair paths can name the recovery they just performed:
    /// one that only the unified solver reached is written down rather than
    /// passed over in silence (decision EC2-103, section 5).
    fn recovery_path(&self, lost: &[bool]) -> RecoveryPath;

    /// Failure matrix for operator-facing documentation and tests.
    fn failure_matrix(&self) -> FailureMatrix;
}

/// Build a portable codec engine for a validated profile snapshot.
///
/// This crate always answers with the pure-Rust engines. An accelerated backend
/// depends on this crate, so it cannot be reached from inside it; the EC runtime
/// wraps this factory with the engine rule from [`crate::select_engine`]. See
/// `s4_ee::ec_codec`.
///
/// # Errors
///
/// Returns [`CodecError::InvalidProfile`] when the profile does not describe a
/// codec this crate can build.
pub fn build_codec(profile: EcProfileSnapshot) -> Result<Box<dyn CodecEngine>, CodecError> {
    match profile.codec {
        EcCodecKind::ReedSolomon => Ok(Box::new(ReedSolomonCodec::new(profile)?)),
        EcCodecKind::LocallyRepairable => Ok(Box::new(LrcCodec::new(profile)?)),
    }
}

/// Codec validation and arithmetic errors.
#[derive(Debug, Error)]
pub enum CodecError {
    /// Invalid profile definition.
    #[error("invalid EC profile '{profile}': {reason}")]
    InvalidProfile {
        /// Profile name.
        profile: String,
        /// Human-readable validation failure.
        reason: String,
    },

    /// Invalid input bytes.
    #[error("invalid codec input: {reason}")]
    InvalidInput {
        /// Human-readable validation failure.
        reason: String,
    },

    /// Invalid shard set.
    #[error("invalid shard set: {reason}")]
    InvalidShardSet {
        /// Human-readable validation failure.
        reason: String,
    },

    /// Shard integrity check failed.
    #[error("hash mismatch for shard slot {slot_index}: {kind}")]
    HashMismatch {
        /// Shard slot index.
        slot_index: u8,
        /// Failed hash kind.
        kind: &'static str,
    },

    /// Not enough valid shards or unsupported loss pattern.
    #[error("not recoverable: {reason}")]
    NotRecoverable {
        /// Human-readable reason.
        reason: String,
    },

    /// Internal finite-field arithmetic failed.
    #[error("codec arithmetic error: {reason}")]
    Arithmetic {
        /// Human-readable reason.
        reason: String,
    },

    /// The requested codec engine cannot be provided by this build or CPU.
    #[error("codec engine unavailable: {reason}")]
    EngineUnavailable {
        /// Human-readable reason, written for an operator reading a start-up log.
        reason: String,
    },
}

pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::new().chain_update(data).finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_thresholds_match_phase_plan() {
        let profile = EcProfileSnapshot::rs_standard();
        assert_eq!(profile.total_shards(), 9);
        assert_eq!(profile.k, 6);
        assert_eq!(profile.role_for_slot(0).unwrap(), ShardRole::Data);
        assert_eq!(
            profile.role_for_slot(8).unwrap(),
            ShardRole::ReedSolomonParity
        );
    }

    #[test]
    fn invalid_profiles_are_rejected() {
        assert!(EcProfileSnapshot::new_rs("bad", 4, 0).is_err());
        assert!(EcProfileSnapshot::new_lrc("bad", 5, 2, 1).is_err());
    }
}
