// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Packed segments for small objects in an erasure-coded pool.
//!
//! An object below `min_object_size` is never erasure coded on its own: it stays
//! `ReplicatedSmall`, which costs three full copies. Packed segments gather such
//! objects into one large container, seal it, and let the existing transcode
//! pipeline encode the container as if it were a single large object.
//!
//! This module owns the container itself and nothing else. It is deliberately
//! runtime independent: no fjall, no gRPC, no clock. Time arrives as a `u64` of
//! nanoseconds since the Unix epoch, durability arrives through
//! [`PackedSegmentJournal`], and every decision is a pure function that a unit
//! test can drive. The durable keyspace, the read path, the transcode target and
//! the repacker live in later phases and build on the types defined here.
//!
//! Two invariants shape everything below.
//!
//! * **An open segment never lowers durability.** While a segment is open the
//!   object bytes stay replicated exactly as `ReplicatedSmall` stores them. The
//!   segment records where an object will sit once the container is encoded; it
//!   is not the only place the bytes live. Encoding and hot-copy purge happen
//!   after sealing, on the pipeline EC v1 already uses for a large object.
//! * **An object is accepted only once its journal record is durable.** The
//!   client is acknowledged after that point and never before, so a crash can
//!   lose an unacknowledged write but never an acknowledged one.
//!
//! Decisions: `EC2-060` (sizing and sealing), `EC2-061` (segment index),
//! `EC2-062` (crash recovery), `EC2-063` (direct read), `EC2-066` (targets).

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use s4_cluster::HlcTimestamp;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::ec_metadata::EcObjectVersionKey;
use crate::erasure_coding::{parse_optional_bool_env, EcConfigError};

/// Environment variable that switches small-object packing on.
pub const PACKED_SEGMENTS_ENABLED_ENV: &str = "S4_EC_PACKED_SEGMENTS_ENABLED";

/// Environment variable that sets the size a segment grows to before sealing.
pub const PACKED_SEGMENT_TARGET_BYTES_ENV: &str = "S4_EC_PACKED_SEGMENT_TARGET_BYTES";

/// Environment variable that caps how long a segment may stay open.
pub const PACKED_SEGMENT_MAX_AGE_SECS_ENV: &str = "S4_EC_PACKED_SEGMENT_MAX_AGE_SECS";

/// Environment variable that caps how many objects one segment may hold.
pub const PACKED_SEGMENT_MAX_OBJECTS_ENV: &str = "S4_EC_PACKED_SEGMENT_MAX_OBJECTS";

/// Environment variable that caps how many segments stay open per erasure set.
pub const PACKED_SEGMENT_MAX_OPEN_ENV: &str = "S4_EC_PACKED_SEGMENT_MAX_OPEN";

/// Default segment size before sealing: 64 MiB.
pub const DEFAULT_PACKED_SEGMENT_TARGET_BYTES: u64 = 64 * 1024 * 1024;

/// Default time a segment may wait for more objects before sealing: 5 minutes.
pub const DEFAULT_PACKED_SEGMENT_MAX_AGE_SECS: u64 = 300;

/// Default upper bound on the number of objects in one segment.
pub const DEFAULT_PACKED_SEGMENT_MAX_OBJECTS: u32 = 4096;

/// Default number of segments kept open per erasure set.
pub const DEFAULT_PACKED_SEGMENT_MAX_OPEN: u32 = 4;

/// Largest segment size an operator may configure.
///
/// Bounded by the encode budget: `DEFAULT_EC_MAX_INFLIGHT_ENCODE_BYTES` is
/// 256 MiB, and one segment must not claim the whole pipeline.
pub const MAX_PACKED_SEGMENT_TARGET_BYTES: u64 = 256 * 1024 * 1024;

/// Smallest segment size, expressed as a multiple of `min_object_size`.
///
/// Below this a segment holds too few objects for the chunk and shard
/// arithmetic of `EC2-063` to select anything.
pub const MIN_PACKED_SEGMENT_TARGET_MULTIPLE: u64 = 8;

/// Longest time a segment may be configured to stay open: one day.
pub const MAX_PACKED_SEGMENT_MAX_AGE_SECS: u64 = 86_400;

/// Largest object count an operator may configure for one segment.
pub const MAX_PACKED_SEGMENT_MAX_OBJECTS: u32 = 65_536;

/// Largest number of segments an operator may keep open per erasure set.
pub const MAX_PACKED_SEGMENT_MAX_OPEN: u32 = 64;

/// Reserved bucket the shards of a packed segment are described under.
///
/// A segment is not an object, but its shards are described by an ordinary
/// [`EcObjectManifest`](crate::ec_metadata::EcObjectManifest), and every EC v1
/// subsystem that touches shards — transcode, repair, scrub, anti-entropy,
/// garbage collection — is keyed by an object version. Giving the segment a key
/// in a reserved namespace lets all of that work on segments unchanged, instead
/// of growing a second copy of each subsystem for a container.
///
/// The name starts with a dot, which S3 forbids in a bucket name, so it cannot
/// collide with a bucket a client created.
pub const PACKED_SEGMENT_MANIFEST_BUCKET: &str = ".s4-packed-segments";

/// The manifest key that describes one segment's shards.
///
/// Derived rather than stored: a segment id is stable for the life of the
/// segment, so the key is too, and a record cannot go out of step with it.
pub fn packed_segment_manifest_key(segment_id: PackedSegmentId) -> EcObjectVersionKey {
    EcObjectVersionKey::new(PACKED_SEGMENT_MANIFEST_BUCKET, segment_id.to_string(), None)
}

/// Read a segment id back out of the key its manifest is stored under.
///
/// The inverse of [`packed_segment_manifest_key`], and the only place that
/// inverse exists: the transcode pipeline is handed a manifest key and has to
/// find the segment record it belongs to.
///
/// # Errors
///
/// Returns [`PackedSegmentError::Invalid`] when the key does not name a segment
/// — a key outside the reserved bucket, or one whose name is not a segment id.
pub fn packed_segment_id_from_manifest_key(
    key: &EcObjectVersionKey,
) -> Result<PackedSegmentId, PackedSegmentError> {
    if key.bucket != PACKED_SEGMENT_MANIFEST_BUCKET {
        return Err(PackedSegmentError::Invalid {
            reason: format!(
                "{}/{} is not a packed-segment manifest key; they live in {}",
                key.bucket, key.key, PACKED_SEGMENT_MANIFEST_BUCKET
            ),
        });
    }
    let uuid = Uuid::parse_str(&key.key).map_err(|error| PackedSegmentError::Invalid {
        reason: format!("'{}' is not a segment id: {error}", key.key),
    })?;
    Ok(PackedSegmentId::from_uuid(uuid))
}

/// Packed segment errors.
#[derive(Debug, Error)]
pub enum PackedSegmentError {
    /// The segment is not open and cannot accept objects.
    #[error("packed segment {segment_id} is {state} and does not accept objects")]
    NotOpen {
        /// Segment that refused the object.
        segment_id: PackedSegmentId,
        /// State the segment is actually in.
        state: PackedSegmentState,
    },

    /// A state transition that the lifecycle does not allow.
    #[error("packed segment {segment_id} cannot move from {from} to {to}")]
    InvalidTransition {
        /// Segment whose transition was refused.
        segment_id: PackedSegmentId,
        /// Current state.
        from: PackedSegmentState,
        /// Requested state.
        to: PackedSegmentState,
    },

    /// The object cannot fit any segment of this size, empty or not.
    #[error("object of {length} byte(s) never fits a packed segment of {target_bytes} byte(s)")]
    ObjectTooLarge {
        /// Object length in bytes.
        length: u64,
        /// Configured segment size.
        target_bytes: u64,
    },

    /// The object does not fit the remaining room in this segment.
    ///
    /// Not a failure on the pool path: the caller seals this segment and opens
    /// a new one, because an object is never split across two segments.
    #[error("packed segment {segment_id} has {remaining} byte(s) left, object needs {length}")]
    SegmentFull {
        /// Segment that ran out of room.
        segment_id: PackedSegmentId,
        /// Bytes still available in the segment.
        remaining: u64,
        /// Bytes the object needs.
        length: u64,
    },

    /// A reservation is already in flight on this segment.
    ///
    /// Appends to one segment are serialized so that an aborted reservation can
    /// always roll the write cursor back exactly. Concurrency comes from having
    /// several segments open, not from interleaving appends inside one.
    #[error("packed segment {segment_id} already holds an uncommitted reservation")]
    ReservationInFlight {
        /// Segment holding the reservation.
        segment_id: PackedSegmentId,
    },

    /// The reservation does not match the segment it is being applied to.
    #[error("packed segment reservation mismatch: {reason}")]
    ReservationMismatch {
        /// Human-readable explanation.
        reason: String,
    },

    /// Every open segment is busy and no new one may be opened.
    #[error("all {open} open packed segment(s) are busy; object stays replicated")]
    PoolExhausted {
        /// Number of segments currently open.
        open: u32,
    },

    /// A segment or entry violates its own invariants.
    #[error("invalid packed segment: {reason}")]
    Invalid {
        /// Human-readable validation failure.
        reason: String,
    },

    /// The durable journal could not be read or written.
    #[error("packed segment journal error: {0}")]
    Journal(String),

    /// Configuration is unusable.
    #[error("packed segment configuration error: {0}")]
    Config(#[from] EcConfigError),
}

/// Configuration of small-object packing.
///
/// Disabled by default. Packing introduces a data lifecycle that a feature flag
/// alone cannot undo — existing segments need the unpacker of `EC2-PACK-06-006`
/// — so the safe default is off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackedSegmentConfig {
    /// Whether small objects are packed into segments at all.
    pub enabled: bool,
    /// Size a segment grows to before it is sealed.
    pub target_bytes: u64,
    /// Time a segment may wait for more objects before it is sealed anyway.
    pub max_age: Duration,
    /// Upper bound on the number of objects in one segment.
    pub max_objects: u32,
    /// Number of segments kept open per erasure set.
    pub max_open_segments: u32,
}

impl Default for PackedSegmentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            target_bytes: DEFAULT_PACKED_SEGMENT_TARGET_BYTES,
            max_age: Duration::from_secs(DEFAULT_PACKED_SEGMENT_MAX_AGE_SECS),
            max_objects: DEFAULT_PACKED_SEGMENT_MAX_OBJECTS,
            max_open_segments: DEFAULT_PACKED_SEGMENT_MAX_OPEN,
        }
    }
}

impl PackedSegmentConfig {
    /// Parse the `S4_EC_PACKED_SEGMENT*` variables.
    ///
    /// Every variable is optional and every default reproduces EC v1 behaviour,
    /// so a deployment that sets none of them is unaffected by this module.
    ///
    /// # Errors
    ///
    /// Returns [`EcConfigError::Env`] when a variable is present but does not
    /// parse, or parses outside the range the decision record allows.
    pub fn from_env() -> Result<Self, EcConfigError> {
        let defaults = Self::default();
        let target_bytes = parse_optional_u64_env(PACKED_SEGMENT_TARGET_BYTES_ENV)?
            .unwrap_or(defaults.target_bytes);
        let max_age_secs = parse_optional_u64_env(PACKED_SEGMENT_MAX_AGE_SECS_ENV)?
            .unwrap_or(DEFAULT_PACKED_SEGMENT_MAX_AGE_SECS);
        let max_objects =
            parse_optional_u32_env(PACKED_SEGMENT_MAX_OBJECTS_ENV)?.unwrap_or(defaults.max_objects);
        let max_open_segments = parse_optional_u32_env(PACKED_SEGMENT_MAX_OPEN_ENV)?
            .unwrap_or(defaults.max_open_segments);

        ensure_range_u64(
            PACKED_SEGMENT_TARGET_BYTES_ENV,
            target_bytes,
            1,
            MAX_PACKED_SEGMENT_TARGET_BYTES,
        )?;
        ensure_range_u64(
            PACKED_SEGMENT_MAX_AGE_SECS_ENV,
            max_age_secs,
            1,
            MAX_PACKED_SEGMENT_MAX_AGE_SECS,
        )?;
        ensure_range_u32(
            PACKED_SEGMENT_MAX_OBJECTS_ENV,
            max_objects,
            1,
            MAX_PACKED_SEGMENT_MAX_OBJECTS,
        )?;
        ensure_range_u32(
            PACKED_SEGMENT_MAX_OPEN_ENV,
            max_open_segments,
            1,
            MAX_PACKED_SEGMENT_MAX_OPEN,
        )?;

        Ok(Self {
            enabled: parse_optional_bool_env(PACKED_SEGMENTS_ENABLED_ENV)?.unwrap_or(false),
            target_bytes,
            max_age: Duration::from_secs(max_age_secs),
            max_objects,
            max_open_segments,
        })
    }

    /// Check the configuration against the codec profile it will run under.
    ///
    /// A segment must hold a useful number of objects, which only makes sense
    /// relative to the threshold below which objects are packed at all.
    ///
    /// # Errors
    ///
    /// Returns [`EcConfigError::Env`] when the segment is too small to be worth
    /// packing for this profile.
    pub fn validate_for_profile(&self, min_object_size: u64) -> Result<(), EcConfigError> {
        let floor = min_object_size.saturating_mul(MIN_PACKED_SEGMENT_TARGET_MULTIPLE);
        if self.target_bytes < floor {
            return Err(EcConfigError::Env {
                name: PACKED_SEGMENT_TARGET_BYTES_ENV,
                reason: format!(
                    "segment size {} is below {floor}, which is {MIN_PACKED_SEGMENT_TARGET_MULTIPLE} \
                     times the profile min_object_size {min_object_size}",
                    self.target_bytes
                ),
            });
        }
        Ok(())
    }

    /// Whether an object of this size belongs in a segment.
    ///
    /// A zero-length object stays `ReplicatedSmall`: it occupies no bytes to
    /// save, and packing it would cost an index entry for nothing.
    pub fn accepts_object_size(&self, size: u64, min_object_size: u64) -> bool {
        self.enabled && size > 0 && size < min_object_size && size <= self.target_bytes
    }

    /// Maximum age as nanoseconds, for comparison against segment timestamps.
    pub fn max_age_nanos(&self) -> u64 {
        let nanos = self.max_age.as_nanos();
        nanos.min(u128::from(u64::MAX)) as u64
    }
}

/// Stable identifier of one packed segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PackedSegmentId(Uuid);

impl PackedSegmentId {
    /// Generate a new random segment identifier.
    pub fn new_random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wrap an existing UUID.
    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// Borrow the underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Whether this identifier is the nil UUID, which is never a valid segment.
    pub fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl fmt::Display for PackedSegmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Lifecycle state of a packed segment.
///
/// The full set is declared here although phase 1 only produces `Open` and
/// `Sealed`. The state is serialized into durable metadata, so introducing
/// variants later would change the format of records already written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackedSegmentState {
    /// Accepting objects. Bytes are durable as `ReplicatedSmall`.
    Open,
    /// Contents frozen. Offsets are final, no further object is accepted.
    Sealed,
    /// Encoded into shards; the manifest is committed and reads come from EC.
    Encoded,
    /// Hot replicated copies purged; EC is the only placement.
    Purged,
    /// Repacked or emptied; the shards are garbage-collectable.
    Dead,
}

impl PackedSegmentState {
    /// Whether a segment in this state still accepts objects.
    pub fn accepts_objects(self) -> bool {
        matches!(self, Self::Open)
    }

    /// Whether the lifecycle allows moving from this state to `next`.
    ///
    /// Every state may go to `Dead`: a segment can be abandoned at any point,
    /// and abandoning it is always safe because the object bytes exist
    /// elsewhere until the hot copies are purged.
    pub fn can_transition_to(self, next: Self) -> bool {
        match (self, next) {
            (Self::Open, Self::Sealed)
            | (Self::Sealed, Self::Encoded)
            | (Self::Encoded, Self::Purged) => true,
            (Self::Dead, _) => false,
            (_, Self::Dead) => true,
            _ => false,
        }
    }
}

impl fmt::Display for PackedSegmentState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Open => "open",
            Self::Sealed => "sealed",
            Self::Encoded => "encoded",
            Self::Purged => "purged",
            Self::Dead => "dead",
        };
        formatter.write_str(name)
    }
}

/// Why a segment was sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SealReason {
    /// The next object did not fit the configured segment size.
    TargetSizeReached,
    /// The segment waited longer than the configured maximum age.
    MaxAgeReached,
    /// The segment reached the configured object count.
    MaxObjectsReached,
    /// An operator sealed the segment through the admin API.
    OperatorRequest,
    /// The segment was assembled by a repack and was sealed the moment it was full.
    ///
    /// A repacked segment is never open to clients: it is built, sealed and made
    /// durable in one batch, so none of the other reasons ever applies to it.
    Repacked,
}

impl fmt::Display for SealReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::TargetSizeReached => "target_size_reached",
            Self::MaxAgeReached => "max_age_reached",
            Self::MaxObjectsReached => "max_objects_reached",
            Self::OperatorRequest => "operator_request",
            Self::Repacked => "repacked",
        };
        formatter.write_str(name)
    }
}

/// Everything about an object that an append records, apart from its place.
///
/// Grouped because these six values always travel together: they are what makes
/// the entry a description of an object rather than of a byte range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedObjectIdentity {
    /// Object version being packed.
    pub object: EcObjectVersionKey,
    /// SHA-256 of the complete object content.
    pub content_hash_sha256: [u8; 32],
    /// HLC timestamp for conflict ordering.
    pub hlc: HlcTimestamp,
    /// S3 ETag.
    pub etag: String,
    /// Content-Type.
    pub content_type: String,
    /// User-defined metadata from S3 headers.
    pub user_metadata: HashMap<String, String>,
}

impl PackedObjectIdentity {
    /// Identify an object by content alone, with no S3 metadata yet.
    ///
    /// An empty ETag is a real production value, not a placeholder: the hot
    /// write coordinator leaves it empty when the quorum was satisfied by
    /// remote replicas only.
    pub fn new(
        object: EcObjectVersionKey,
        content_hash_sha256: [u8; 32],
        hlc: HlcTimestamp,
    ) -> Self {
        Self {
            object,
            content_hash_sha256,
            hlc,
            etag: String::new(),
            content_type: String::new(),
            user_metadata: HashMap::new(),
        }
    }

    /// Attach the S3 metadata a client must see again after the segment is encoded.
    pub fn with_s3_metadata(
        mut self,
        etag: impl Into<String>,
        content_type: impl Into<String>,
        user_metadata: HashMap<String, String>,
    ) -> Self {
        self.etag = etag.into();
        self.content_type = content_type.into();
        self.user_metadata = user_metadata;
        self
    }
}

/// One object inside a packed segment.
///
/// `content_hash_sha256` repeats a hash the object metadata already carries.
/// The duplication is deliberate: a direct read (`EC2-063`) verifies the bytes
/// it assembled against this entry alone, without a second metadata lookup.
///
/// The S3 metadata is here for a blunter reason. Once the segment is encoded and
/// the object's hot copies are purged, nothing else on the node still describes
/// the object: the segment's manifest describes the container, not what is
/// inside it. Without ETag and Content-Type stored here a `HEAD` after the purge
/// would answer differently from one before it, which the client-invariance rule
/// of the EC v2 conventions forbids outright.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackedSegmentEntry {
    /// Object version stored at this offset.
    pub object: EcObjectVersionKey,
    /// Offset of the object from the start of the segment.
    pub offset: u64,
    /// Object length in bytes.
    pub length: u64,
    /// SHA-256 of the complete object content.
    pub content_hash_sha256: [u8; 32],
    /// HLC timestamp for conflict ordering.
    pub hlc: HlcTimestamp,
    /// S3 ETag.
    pub etag: String,
    /// Content-Type.
    pub content_type: String,
    /// User-defined metadata from S3 headers.
    pub user_metadata: HashMap<String, String>,
}

impl PackedSegmentEntry {
    /// First offset past this object.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::Invalid`] when the sum overflows `u64`.
    pub fn end_offset(&self) -> Result<u64, PackedSegmentError> {
        self.offset.checked_add(self.length).ok_or_else(|| PackedSegmentError::Invalid {
            reason: format!(
                "entry for {}/{} at offset {} with length {} overflows u64",
                self.object.bucket, self.object.key, self.offset, self.length
            ),
        })
    }

    /// Validate the entry before it is written durably.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::Invalid`] when the object key is unusable
    /// or the object is empty, since an empty object is never packed.
    pub fn validate(&self) -> Result<(), PackedSegmentError> {
        self.object.validate().map_err(|error| PackedSegmentError::Invalid {
            reason: error.to_string(),
        })?;
        if self.length == 0 {
            return Err(PackedSegmentError::Invalid {
                reason: format!(
                    "entry for {}/{} has zero length; empty objects are not packed",
                    self.object.bucket, self.object.key
                ),
            });
        }
        self.end_offset()?;
        Ok(())
    }
}

/// Room reserved in an open segment for one object that is not yet committed.
///
/// The reservation is the bridge between "we know where the object will sit"
/// and "the object is durable". It is handed out before the bytes are made
/// durable and applied only after the journal record has been persisted, which
/// is what keeps the acknowledgement honest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackedSegmentReservation {
    /// Segment holding the reservation.
    pub segment_id: PackedSegmentId,
    /// Offset the object will occupy.
    pub offset: u64,
    /// Length the object will occupy.
    pub length: u64,
    /// Journal sequence number this append will use.
    pub sequence: u64,
}

/// A packed segment and its index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackedSegment {
    /// Stable segment identifier.
    pub segment_id: PackedSegmentId,
    /// Erasure set that will own the shards of this segment.
    pub erasure_set_id: u64,
    /// Lifecycle state.
    pub state: PackedSegmentState,
    /// Segment size this segment was opened with.
    ///
    /// A snapshot, not a live reading of the configuration: an operator may
    /// change the variable while a segment is open, and the segment has to keep
    /// living by the rules it was opened under, or its own offsets stop
    /// agreeing with its own sealing condition.
    pub target_bytes: u64,
    /// Object count limit this segment was opened with.
    pub max_objects: u32,
    /// Bytes handed out to objects, including bytes of deleted objects.
    ///
    /// Never decreases. It describes the physical extent of the segment, not
    /// its occupancy — the dead fraction of `EC2-061` is measured against it,
    /// and a shrinking denominator would make that fraction meaningless.
    pub used_bytes: u64,
    /// Bytes belonging to objects that have since been deleted.
    pub dead_bytes: u64,
    /// Live objects, ordered by offset.
    pub entries: Vec<PackedSegmentEntry>,
    /// Creation time in nanoseconds since the Unix epoch.
    pub created_at: u64,
    /// Sealing time in nanoseconds since the Unix epoch.
    pub sealed_at: Option<u64>,
    /// Why the segment was sealed.
    pub seal_reason: Option<SealReason>,
    /// Next journal sequence number for this segment.
    pub next_sequence: u64,
    /// Segment this one was repacked out of, when it is a replacement.
    ///
    /// A replacement is built from another segment's live entries and its
    /// objects still point at that other segment until the cutover. The link is
    /// what lets a node that restarted in the middle finish the repack instead
    /// of leaving two segments holding the same objects.
    pub repacked_from: Option<PackedSegmentId>,
    /// When the segment was retired, in nanoseconds since the Unix epoch.
    ///
    /// Set exactly when the state becomes `Dead` and never cleared. It starts
    /// the drain window of `EC2-062`: a read that resolved its layout just
    /// before the repack cutover is still reading these shards, and the segment
    /// may not be tombstoned until such a read cannot still be running.
    pub dead_at: Option<u64>,
    /// Reservation waiting for its journal record to become durable.
    #[serde(skip)]
    pending: Option<PackedSegmentReservation>,
}

impl PackedSegment {
    /// Open a new segment under the given configuration.
    pub fn open(
        segment_id: PackedSegmentId,
        erasure_set_id: u64,
        config: &PackedSegmentConfig,
        created_at: u64,
    ) -> Self {
        Self {
            segment_id,
            erasure_set_id,
            state: PackedSegmentState::Open,
            target_bytes: config.target_bytes,
            max_objects: config.max_objects,
            used_bytes: 0,
            dead_bytes: 0,
            entries: Vec::new(),
            created_at,
            sealed_at: None,
            seal_reason: None,
            next_sequence: 1,
            repacked_from: None,
            dead_at: None,
            pending: None,
        }
    }

    /// Open a replacement segment for the live contents of another one.
    ///
    /// Sized to hold everything the source still has live, even when that is
    /// more than the current configuration would allow a fresh segment. A
    /// repack that spilled into two replacements could not switch every object
    /// in one operation, and a half-switched version list is exactly what
    /// `EC2-065` forbids — so the replacement is made big enough that it never
    /// has to.
    pub fn open_repack(
        segment_id: PackedSegmentId,
        source: &PackedSegment,
        config: &PackedSegmentConfig,
        created_at: u64,
    ) -> Self {
        let mut segment = Self::open(segment_id, source.erasure_set_id, config, created_at);
        segment.target_bytes = config.target_bytes.max(source.live_bytes()).max(1);
        segment.max_objects = config.max_objects.max(source.entries.len() as u32);
        segment.repacked_from = Some(source.segment_id);
        segment
    }

    /// The manifest key this segment's shards are described under.
    pub fn manifest_object_key(&self) -> EcObjectVersionKey {
        packed_segment_manifest_key(self.segment_id)
    }

    /// The index entry for one object, if this segment still holds it.
    pub fn entry_for(&self, object: &EcObjectVersionKey) -> Option<&PackedSegmentEntry> {
        self.entries.iter().find(|entry| &entry.object == object)
    }

    /// Bytes still available for new objects.
    pub fn remaining_bytes(&self) -> u64 {
        self.target_bytes.saturating_sub(self.reserved_bytes())
    }

    /// Bytes of live objects, that is everything handed out minus the dead part.
    pub fn live_bytes(&self) -> u64 {
        self.used_bytes.saturating_sub(self.dead_bytes)
    }

    /// Fraction of the segment occupied by deleted objects, in `0.0..=1.0`.
    ///
    /// An empty segment reports `0.0`: there is nothing to reclaim from it.
    pub fn dead_ratio(&self) -> f64 {
        if self.used_bytes == 0 {
            return 0.0;
        }
        (self.dead_bytes as f64) / (self.used_bytes as f64)
    }

    /// Whether an append is waiting for its journal record.
    pub fn has_pending_reservation(&self) -> bool {
        self.pending.is_some()
    }

    /// Whether this segment could take an object of this length right now.
    pub fn has_room_for(&self, length: u64) -> bool {
        self.state.accepts_objects()
            && self.pending.is_none()
            && length > 0
            && length <= self.remaining_bytes()
            && (self.entries.len() as u64) < u64::from(self.max_objects)
    }

    /// Sealing condition that already holds, if any.
    ///
    /// Only the conditions that do not depend on an incoming object are
    /// checked here. Running out of room is decided by [`Self::reserve`], which
    /// is the only place that knows how large the next object is.
    pub fn seal_condition(&self, now: u64, config: &PackedSegmentConfig) -> Option<SealReason> {
        if !self.state.accepts_objects() {
            return None;
        }
        if (self.entries.len() as u64) >= u64::from(self.max_objects) {
            return Some(SealReason::MaxObjectsReached);
        }
        if now.saturating_sub(self.created_at) >= config.max_age_nanos() {
            return Some(SealReason::MaxAgeReached);
        }
        None
    }

    /// Reserve room for one object.
    ///
    /// Reserving does not make the object part of the segment. The caller makes
    /// the bytes durable, persists the journal record from
    /// [`Self::journal_append`], and only then calls [`Self::commit`].
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::NotOpen`] for a sealed segment,
    /// [`PackedSegmentError::ReservationInFlight`] when an append is already
    /// waiting, [`PackedSegmentError::ObjectTooLarge`] when no segment of this
    /// size could ever hold the object, and [`PackedSegmentError::SegmentFull`]
    /// when this particular segment has run out of room or entries.
    pub fn reserve(&mut self, length: u64) -> Result<PackedSegmentReservation, PackedSegmentError> {
        if !self.state.accepts_objects() {
            return Err(PackedSegmentError::NotOpen {
                segment_id: self.segment_id,
                state: self.state,
            });
        }
        if self.pending.is_some() {
            return Err(PackedSegmentError::ReservationInFlight {
                segment_id: self.segment_id,
            });
        }
        if length == 0 || length > self.target_bytes {
            return Err(PackedSegmentError::ObjectTooLarge {
                length,
                target_bytes: self.target_bytes,
            });
        }
        if (self.entries.len() as u64) >= u64::from(self.max_objects) {
            return Err(PackedSegmentError::SegmentFull {
                segment_id: self.segment_id,
                remaining: 0,
                length,
            });
        }
        let remaining = self.remaining_bytes();
        if length > remaining {
            return Err(PackedSegmentError::SegmentFull {
                segment_id: self.segment_id,
                remaining,
                length,
            });
        }

        let reservation = PackedSegmentReservation {
            segment_id: self.segment_id,
            offset: self.used_bytes,
            length,
            sequence: self.next_sequence,
        };
        self.pending = Some(reservation);
        Ok(reservation)
    }

    /// Build the journal record that makes a reservation durable.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::ReservationMismatch`] when the reservation
    /// is not the one this segment is holding, and
    /// [`PackedSegmentError::Invalid`] when the resulting entry is malformed.
    pub fn journal_append(
        &self,
        reservation: &PackedSegmentReservation,
        identity: PackedObjectIdentity,
    ) -> Result<PackedSegmentJournalItem, PackedSegmentError> {
        self.ensure_pending_matches(reservation)?;
        let entry = PackedSegmentEntry {
            object: identity.object,
            offset: reservation.offset,
            length: reservation.length,
            content_hash_sha256: identity.content_hash_sha256,
            hlc: identity.hlc,
            etag: identity.etag,
            content_type: identity.content_type,
            user_metadata: identity.user_metadata,
        };
        entry.validate()?;
        Ok(PackedSegmentJournalItem {
            segment_id: self.segment_id,
            sequence: reservation.sequence,
            payload: PackedSegmentJournalPayload::ObjectAppended {
                entry: Box::new(entry),
            },
        })
    }

    /// Apply an append whose journal record is already durable.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::ReservationMismatch`] when the item does
    /// not describe the reservation this segment is holding.
    pub fn commit(
        &mut self,
        reservation: &PackedSegmentReservation,
        item: &PackedSegmentJournalItem,
    ) -> Result<&PackedSegmentEntry, PackedSegmentError> {
        self.ensure_pending_matches(reservation)?;
        let PackedSegmentJournalPayload::ObjectAppended { entry } = &item.payload else {
            return Err(PackedSegmentError::ReservationMismatch {
                reason: format!(
                    "journal item for segment {} is not an append",
                    self.segment_id
                ),
            });
        };
        if item.segment_id != self.segment_id || item.sequence != reservation.sequence {
            return Err(PackedSegmentError::ReservationMismatch {
                reason: format!(
                    "journal item {}#{} does not match reservation {}#{}",
                    item.segment_id, item.sequence, reservation.segment_id, reservation.sequence
                ),
            });
        }
        if entry.offset != reservation.offset || entry.length != reservation.length {
            return Err(PackedSegmentError::ReservationMismatch {
                reason: format!(
                    "journal item places the object at {}+{} but the reservation is {}+{}",
                    entry.offset, entry.length, reservation.offset, reservation.length
                ),
            });
        }

        self.pending = None;
        self.apply_append(entry.as_ref().clone())?;
        self.entries.last().ok_or_else(|| PackedSegmentError::Invalid {
            reason: format!("segment {} lost the entry it just added", self.segment_id),
        })
    }

    /// Drop a reservation whose object never became durable.
    ///
    /// The write cursor rolls back exactly, because a segment holds at most one
    /// reservation at a time and it is always the last one.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::ReservationMismatch`] when the reservation
    /// is not the one this segment is holding.
    pub fn abort(
        &mut self,
        reservation: &PackedSegmentReservation,
    ) -> Result<(), PackedSegmentError> {
        self.ensure_pending_matches(reservation)?;
        self.pending = None;
        Ok(())
    }

    /// Build the journal record that seals this segment.
    pub fn journal_seal(&self, sealed_at: u64, reason: SealReason) -> PackedSegmentJournalItem {
        PackedSegmentJournalItem {
            segment_id: self.segment_id,
            sequence: self.next_sequence,
            payload: PackedSegmentJournalPayload::SegmentSealed { sealed_at, reason },
        }
    }

    /// Charge a deleted object to this segment.
    ///
    /// Returns the number of bytes charged to the dead count.
    ///
    /// Deletion is deliberately not a journal record. A journal record needs a
    /// sequence number, a sequence number needs a single writer, and a delete
    /// can arrive at any node in the cluster. A removal is instead idempotent
    /// and order-free — "this object is dead in this segment" commutes with
    /// everything else — so it is recorded as a durable set entry and applied
    /// here when the segment is rebuilt. See `EC2-062`.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::Invalid`] when the segment does not hold
    /// the object.
    pub fn record_object_deleted(
        &mut self,
        object: &EcObjectVersionKey,
    ) -> Result<u64, PackedSegmentError> {
        self.remove_entry(object)
    }

    /// Build the journal record that opens this segment.
    pub fn journal_open(&self) -> PackedSegmentJournalItem {
        PackedSegmentJournalItem {
            segment_id: self.segment_id,
            sequence: 0,
            payload: PackedSegmentJournalPayload::SegmentOpened {
                erasure_set_id: self.erasure_set_id,
                target_bytes: self.target_bytes,
                max_objects: self.max_objects,
                created_at: self.created_at,
            },
        }
    }

    /// Seal the segment, freezing its contents.
    ///
    /// A pending reservation is dropped: it was never acknowledged, so nothing
    /// is lost by refusing it now.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::InvalidTransition`] when the segment is
    /// not open.
    pub fn seal(&mut self, sealed_at: u64, reason: SealReason) -> Result<(), PackedSegmentError> {
        self.transition_to(PackedSegmentState::Sealed)?;
        self.pending = None;
        self.sealed_at = Some(sealed_at);
        self.seal_reason = Some(reason);
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(())
    }

    /// Move the segment to the next lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::InvalidTransition`] when the lifecycle
    /// does not allow the move.
    pub fn transition_to(&mut self, next: PackedSegmentState) -> Result<(), PackedSegmentError> {
        if !self.state.can_transition_to(next) {
            return Err(PackedSegmentError::InvalidTransition {
                segment_id: self.segment_id,
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        Ok(())
    }

    /// Retire the segment, starting the drain window before its shards may go.
    ///
    /// Retiring is not deleting. The record, its index and its shards all stay
    /// exactly where they are; what changes is that nothing points at them any
    /// more. A read that resolved its layout a moment before the repack cutover
    /// is still reading these shards, and [`Self::is_drained`] is what says when
    /// no such read can still be running (`EC2-062`).
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::InvalidTransition`] when the segment is
    /// already dead.
    pub fn retire(&mut self, now: u64) -> Result<(), PackedSegmentError> {
        self.transition_to(PackedSegmentState::Dead)?;
        self.dead_at = Some(now);
        Ok(())
    }

    /// Whether every read that could still be holding this segment has finished.
    ///
    /// Answers `false` for a segment that is not dead: a live segment is never
    /// drained, because new reads keep arriving.
    pub fn is_drained(&self, now: u64, grace_nanos: u64) -> bool {
        let Some(dead_at) = self.dead_at else {
            return false;
        };
        now.saturating_sub(dead_at) >= grace_nanos
    }

    /// Append an object to a segment that is built and persisted in one piece.
    ///
    /// The client write path reserves room, makes the bytes durable, writes a
    /// journal record and only then commits, because each of those steps has to
    /// survive a crash on its own — the client is waiting on the last of them.
    /// A repacked segment has no client waiting: it is assembled whole, sealed,
    /// and made durable in a single batch together with the layouts that point
    /// into it. Nothing exists in between to recover, so nothing needs a
    /// journal.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::reserve`] when the object does not
    /// fit, and [`PackedSegmentError::Invalid`] when the resulting entry is
    /// malformed.
    pub fn append_without_journal(
        &mut self,
        identity: PackedObjectIdentity,
        length: u64,
    ) -> Result<&PackedSegmentEntry, PackedSegmentError> {
        let reservation = self.reserve(length)?;
        let item = match self.journal_append(&reservation, identity) {
            Ok(item) => item,
            Err(error) => {
                // Roll the cursor back, so a rejected object does not leave the
                // segment holding a reservation nobody will ever commit.
                self.abort(&reservation)?;
                return Err(error);
            }
        };
        self.commit(&reservation, &item)
    }

    /// Check every invariant this segment is required to hold.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::Invalid`] describing the first violation.
    pub fn validate(&self) -> Result<(), PackedSegmentError> {
        if self.segment_id.is_nil() {
            return Err(PackedSegmentError::Invalid {
                reason: "segment id must not be nil".to_string(),
            });
        }
        if self.target_bytes == 0 {
            return Err(PackedSegmentError::Invalid {
                reason: format!("segment {} has a zero target size", self.segment_id),
            });
        }
        if self.entries.len() as u64 > u64::from(self.max_objects) {
            return Err(PackedSegmentError::Invalid {
                reason: format!(
                    "segment {} holds {} entries, above its limit of {}",
                    self.segment_id,
                    self.entries.len(),
                    self.max_objects
                ),
            });
        }
        if self.used_bytes > self.target_bytes {
            return Err(PackedSegmentError::Invalid {
                reason: format!(
                    "segment {} used {} byte(s) of a {} byte target",
                    self.segment_id, self.used_bytes, self.target_bytes
                ),
            });
        }
        if self.dead_bytes > self.used_bytes {
            return Err(PackedSegmentError::Invalid {
                reason: format!(
                    "segment {} reports {} dead byte(s) of {} used",
                    self.segment_id, self.dead_bytes, self.used_bytes
                ),
            });
        }

        let mut live_bytes = 0u64;
        let mut previous_end = 0u64;
        for entry in &self.entries {
            entry.validate()?;
            if entry.offset < previous_end {
                return Err(PackedSegmentError::Invalid {
                    reason: format!(
                        "segment {} entry at offset {} overlaps the previous entry ending at {}",
                        self.segment_id, entry.offset, previous_end
                    ),
                });
            }
            previous_end = entry.end_offset()?;
            if previous_end > self.used_bytes {
                return Err(PackedSegmentError::Invalid {
                    reason: format!(
                        "segment {} entry ends at {} beyond the used extent {}",
                        self.segment_id, previous_end, self.used_bytes
                    ),
                });
            }
            live_bytes = live_bytes.saturating_add(entry.length);
        }

        if live_bytes.saturating_add(self.dead_bytes) != self.used_bytes {
            return Err(PackedSegmentError::Invalid {
                reason: format!(
                    "segment {} accounts for {} live plus {} dead byte(s), but used {}",
                    self.segment_id, live_bytes, self.dead_bytes, self.used_bytes
                ),
            });
        }

        match self.state {
            PackedSegmentState::Open => {
                if self.sealed_at.is_some() || self.seal_reason.is_some() {
                    return Err(PackedSegmentError::Invalid {
                        reason: format!("open segment {} carries a seal record", self.segment_id),
                    });
                }
            }
            _ => {
                if self.sealed_at.is_none() || self.seal_reason.is_none() {
                    return Err(PackedSegmentError::Invalid {
                        reason: format!(
                            "segment {} is {} but was never sealed",
                            self.segment_id, self.state
                        ),
                    });
                }
            }
        }

        if (self.state == PackedSegmentState::Dead) != self.dead_at.is_some() {
            return Err(PackedSegmentError::Invalid {
                reason: format!(
                    "segment {} is {} and carries {} retirement time; a retirement time and \
                     the dead state go together or not at all",
                    self.segment_id,
                    self.state,
                    if self.dead_at.is_some() { "a" } else { "no" }
                ),
            });
        }
        Ok(())
    }

    fn reserved_bytes(&self) -> u64 {
        match &self.pending {
            Some(reservation) => self.used_bytes.saturating_add(reservation.length),
            None => self.used_bytes,
        }
    }

    fn ensure_pending_matches(
        &self,
        reservation: &PackedSegmentReservation,
    ) -> Result<(), PackedSegmentError> {
        match &self.pending {
            Some(pending) if pending == reservation => Ok(()),
            Some(pending) => Err(PackedSegmentError::ReservationMismatch {
                reason: format!(
                    "segment {} holds reservation {}+{} but was given {}+{}",
                    self.segment_id,
                    pending.offset,
                    pending.length,
                    reservation.offset,
                    reservation.length
                ),
            }),
            None => Err(PackedSegmentError::ReservationMismatch {
                reason: format!("segment {} holds no reservation", self.segment_id),
            }),
        }
    }

    /// Append an entry that is already durable, growing the write cursor.
    fn apply_append(&mut self, entry: PackedSegmentEntry) -> Result<(), PackedSegmentError> {
        entry.validate()?;
        if entry.offset != self.used_bytes {
            return Err(PackedSegmentError::Invalid {
                reason: format!(
                    "segment {} expects the next object at {} but the entry claims {}",
                    self.segment_id, self.used_bytes, entry.offset
                ),
            });
        }
        let end = entry.end_offset()?;
        if end > self.target_bytes {
            return Err(PackedSegmentError::Invalid {
                reason: format!(
                    "segment {} entry ends at {} beyond its {} byte target",
                    self.segment_id, end, self.target_bytes
                ),
            });
        }
        self.used_bytes = end;
        self.entries.push(entry);
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(())
    }

    /// Drop one entry and charge its bytes to the dead count.
    ///
    /// `used_bytes` stays where it is on purpose: it measures how far the write
    /// cursor got, and the dead fraction is measured against it. A shrinking
    /// denominator would make the repack threshold of `EC2-061` meaningless.
    fn remove_entry(&mut self, object: &EcObjectVersionKey) -> Result<u64, PackedSegmentError> {
        let Some(position) = self.entries.iter().position(|entry| &entry.object == object) else {
            return Err(PackedSegmentError::Invalid {
                reason: format!(
                    "segment {} does not hold {}/{}",
                    self.segment_id, object.bucket, object.key
                ),
            });
        };
        let removed = self.entries.remove(position);
        self.dead_bytes = self.dead_bytes.saturating_add(removed.length);
        Ok(removed.length)
    }
}

/// What one journal record says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PackedSegmentJournalPayload {
    /// A segment was opened.
    SegmentOpened {
        /// Erasure set the segment belongs to.
        erasure_set_id: u64,
        /// Segment size snapshot.
        target_bytes: u64,
        /// Object count snapshot.
        max_objects: u32,
        /// Creation time in nanoseconds since the Unix epoch.
        created_at: u64,
    },
    /// An object was accepted into the segment.
    ///
    /// Durability of this record is what makes the object acknowledgeable.
    ///
    /// Boxed because the entry carries the object's key and S3 metadata and so
    /// dwarfs every other record; without the box each journal record would be
    /// sized for the largest one. Serialization is unaffected — a `Box` is
    /// transparent to serde, so the wire format is the entry itself.
    ObjectAppended {
        /// Index entry for the accepted object.
        entry: Box<PackedSegmentEntry>,
    },
    /// The segment was sealed.
    SegmentSealed {
        /// Sealing time in nanoseconds since the Unix epoch.
        sealed_at: u64,
        /// Why the segment was sealed.
        reason: SealReason,
    },
    /// A record that could not be decoded.
    ///
    /// The journal reader produces this instead of hiding the damage. Replay
    /// stops here, because everything after an unreadable record is a torn
    /// tail: it was written by a node that did not survive to acknowledge it.
    Unreadable {
        /// Human-readable reason the record could not be decoded.
        reason: String,
    },
}

/// One journal record, addressed by segment and sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackedSegmentJournalItem {
    /// Segment this record belongs to.
    pub segment_id: PackedSegmentId,
    /// Position of this record in the segment's journal, starting at zero.
    pub sequence: u64,
    /// What the record says.
    pub payload: PackedSegmentJournalPayload,
}

/// Durable log behind an open segment.
///
/// The port is deliberately narrow. Phase 1 needs only "make this record
/// durable" and "give me everything you have"; the fjall-backed implementation
/// arrives with the segment keyspace in phase 2.
#[async_trait]
pub trait PackedSegmentJournal: Send + Sync {
    /// Make one record durable.
    ///
    /// Returning `Ok` means the record survives a crash. The caller may
    /// acknowledge the object to the client only after that.
    async fn append(&self, item: &PackedSegmentJournalItem) -> Result<(), PackedSegmentError>;

    /// Read every record the journal holds, in write order per segment.
    ///
    /// Records that cannot be decoded are returned as
    /// [`PackedSegmentJournalPayload::Unreadable`] rather than skipped, so that
    /// recovery can tell a torn tail from a missing record.
    async fn load(&self) -> Result<Vec<PackedSegmentJournalItem>, PackedSegmentError>;
}

/// A journal tail that recovery refused to replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedSegmentDiscardedTail {
    /// Segment whose tail was dropped.
    pub segment_id: PackedSegmentId,
    /// Sequence number of the first record that was not replayed.
    pub from_sequence: u64,
    /// How many records were dropped, including the first one.
    pub discarded_records: usize,
    /// Why replay stopped here.
    pub reason: String,
}

/// A journal that recovery refused to trust at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedSegmentRejected {
    /// Segment whose journal was rejected.
    pub segment_id: PackedSegmentId,
    /// Why the journal was rejected.
    pub reason: String,
}

/// Result of replaying a packed segment journal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackedSegmentRecovery {
    /// Segments restored to the last acknowledged object.
    pub segments: Vec<PackedSegment>,
    /// Tails dropped because they could not have been acknowledged.
    pub discarded: Vec<PackedSegmentDiscardedTail>,
    /// Journals that were damaged beyond the point of replay.
    pub rejected: Vec<PackedSegmentRejected>,
}

/// Rebuild segments from a journal.
///
/// Replay stops for a segment at the first record that cannot be decoded, is
/// out of sequence, or contradicts the segment state. Everything from that
/// point on is dropped, because a record that was made durable is by definition
/// readable and in order — a record that is not was still being written when
/// the node died, and its object was therefore never acknowledged.
///
/// Dropping the tail rather than skipping the bad record and continuing is the
/// whole point. Skipping would leave the segment's write cursor disagreeing
/// with its own index, and would do so silently.
pub fn recover_segments(items: Vec<PackedSegmentJournalItem>) -> PackedSegmentRecovery {
    let mut recovery = PackedSegmentRecovery::default();
    for (segment_id, mut journal) in group_by_segment(items) {
        journal.sort_by_key(|item| item.sequence);
        match replay_one_segment(segment_id, &journal) {
            Ok((segment, discarded)) => {
                recovery.segments.push(segment);
                if let Some(tail) = discarded {
                    recovery.discarded.push(tail);
                }
            }
            Err(reason) => recovery.rejected.push(PackedSegmentRejected { segment_id, reason }),
        }
    }
    recovery.segments.sort_by_key(|segment| segment.segment_id);
    recovery.discarded.sort_by_key(|tail| tail.segment_id);
    recovery.rejected.sort_by_key(|rejected| rejected.segment_id);
    recovery
}

/// Open segments of one erasure set.
///
/// The pool is what turns "this segment is full" into "seal it and open the
/// next one", and it is what keeps several segments open so that appends to
/// different segments proceed in parallel. Appends inside one segment stay
/// serialized: a segment holds at most one reservation, so aborting it always
/// rolls the write cursor back exactly.
#[derive(Debug)]
pub struct PackedSegmentPool {
    erasure_set_id: u64,
    config: PackedSegmentConfig,
    open: Vec<PackedSegment>,
    sealed: Vec<PackedSegment>,
}

/// Where the pool placed an object, and what the caller must persist first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedSegmentAdmission {
    /// Room reserved for the object.
    pub reservation: PackedSegmentReservation,
    /// Records that must become durable before the append record does.
    ///
    /// Sealing an old segment and opening a new one are journal events in their
    /// own right. They are handed back rather than written here, because this
    /// module owns no durability of its own.
    pub prerequisites: Vec<PackedSegmentJournalItem>,
}

impl PackedSegmentPool {
    /// Create an empty pool for one erasure set.
    pub fn new(erasure_set_id: u64, config: PackedSegmentConfig) -> Self {
        Self {
            erasure_set_id,
            config,
            open: Vec::new(),
            sealed: Vec::new(),
        }
    }

    /// Rebuild a pool from recovered segments.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::Invalid`] when a segment belongs to a
    /// different erasure set or violates its own invariants.
    pub fn from_recovered(
        erasure_set_id: u64,
        config: PackedSegmentConfig,
        segments: Vec<PackedSegment>,
    ) -> Result<Self, PackedSegmentError> {
        let mut pool = Self::new(erasure_set_id, config);
        for segment in segments {
            segment.validate()?;
            if segment.erasure_set_id != erasure_set_id {
                return Err(PackedSegmentError::Invalid {
                    reason: format!(
                        "segment {} belongs to erasure set {}, not {erasure_set_id}",
                        segment.segment_id, segment.erasure_set_id
                    ),
                });
            }
            match segment.state {
                PackedSegmentState::Open => pool.open.push(segment),
                PackedSegmentState::Sealed => pool.sealed.push(segment),
                _ => {}
            }
        }
        Ok(pool)
    }

    /// Erasure set this pool writes into.
    pub fn erasure_set_id(&self) -> u64 {
        self.erasure_set_id
    }

    /// Configuration this pool runs under.
    pub fn config(&self) -> &PackedSegmentConfig {
        &self.config
    }

    /// Segments currently accepting objects.
    pub fn open_segments(&self) -> &[PackedSegment] {
        &self.open
    }

    /// Segments sealed but not yet handed to the transcode pipeline.
    pub fn sealed_segments(&self) -> &[PackedSegment] {
        &self.sealed
    }

    /// Place one object, sealing and opening segments as needed.
    ///
    /// An object that does not fit the segment it was offered does not get
    /// split: that segment is sealed and the object starts a new one.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::ObjectTooLarge`] when no segment of the
    /// configured size could hold the object, and
    /// [`PackedSegmentError::PoolExhausted`] when every open segment is already
    /// holding a reservation and no further segment may be opened. The caller
    /// then keeps the object `ReplicatedSmall`, which is always correct.
    pub fn reserve(
        &mut self,
        length: u64,
        now: u64,
    ) -> Result<PackedSegmentAdmission, PackedSegmentError> {
        if length == 0 || length > self.config.target_bytes {
            return Err(PackedSegmentError::ObjectTooLarge {
                length,
                target_bytes: self.config.target_bytes,
            });
        }

        let mut prerequisites = self.seal_expired(now);

        // Seal every open segment that cannot take this object, so that a full
        // segment does not sit in the way of the ones that still have room.
        let mut index = 0;
        while index < self.open.len() {
            let segment = &self.open[index];
            if segment.has_pending_reservation() || segment.has_room_for(length) {
                index += 1;
                continue;
            }
            let reason = if (segment.entries.len() as u64) >= u64::from(segment.max_objects) {
                SealReason::MaxObjectsReached
            } else {
                SealReason::TargetSizeReached
            };
            prerequisites.extend(self.seal_at(index, now, reason)?);
        }

        if let Some(segment) = self.open.iter_mut().find(|segment| segment.has_room_for(length)) {
            let reservation = segment.reserve(length)?;
            return Ok(PackedSegmentAdmission {
                reservation,
                prerequisites,
            });
        }

        if (self.open.len() as u64) >= u64::from(self.config.max_open_segments) {
            return Err(PackedSegmentError::PoolExhausted {
                open: self.open.len() as u32,
            });
        }

        let mut segment = PackedSegment::open(
            PackedSegmentId::new_random(),
            self.erasure_set_id,
            &self.config,
            now,
        );
        prerequisites.push(segment.journal_open());
        let reservation = segment.reserve(length)?;
        self.open.push(segment);
        Ok(PackedSegmentAdmission {
            reservation,
            prerequisites,
        })
    }

    /// Build the append record for a reservation this pool handed out.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::ReservationMismatch`] when the reservation
    /// names a segment this pool does not hold open.
    pub fn journal_append(
        &self,
        reservation: &PackedSegmentReservation,
        identity: PackedObjectIdentity,
    ) -> Result<PackedSegmentJournalItem, PackedSegmentError> {
        self.segment(reservation.segment_id)?.journal_append(reservation, identity)
    }

    /// Apply an append whose journal record is durable.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::ReservationMismatch`] when the reservation
    /// or the record does not match the segment.
    pub fn commit(
        &mut self,
        reservation: &PackedSegmentReservation,
        item: &PackedSegmentJournalItem,
    ) -> Result<PackedSegmentEntry, PackedSegmentError> {
        let segment = self.segment_mut(reservation.segment_id)?;
        segment.commit(reservation, item).cloned()
    }

    /// Drop a reservation whose object never became durable.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::ReservationMismatch`] when the reservation
    /// is not the one the segment is holding.
    pub fn abort(
        &mut self,
        reservation: &PackedSegmentReservation,
    ) -> Result<(), PackedSegmentError> {
        self.segment_mut(reservation.segment_id)?.abort(reservation)
    }

    /// Seal every open segment that has waited longer than the configured age.
    ///
    /// Returns the journal records the caller must persist.
    pub fn seal_expired(&mut self, now: u64) -> Vec<PackedSegmentJournalItem> {
        let mut items = Vec::new();
        let mut index = 0;
        while index < self.open.len() {
            let Some(reason) = self.open[index].seal_condition(now, &self.config) else {
                index += 1;
                continue;
            };
            match self.seal_at(index, now, reason) {
                Ok(sealed) => items.extend(sealed),
                // `seal_condition` already established that the segment is
                // open, so the transition cannot be refused. Advancing keeps
                // the loop finite if that ever stops being true.
                Err(_) => index += 1,
            }
        }
        items
    }

    /// Seal one open segment on operator request.
    ///
    /// # Errors
    ///
    /// Returns [`PackedSegmentError::Invalid`] when the pool holds no open
    /// segment with this identifier.
    pub fn seal_now(
        &mut self,
        segment_id: PackedSegmentId,
        now: u64,
        reason: SealReason,
    ) -> Result<Vec<PackedSegmentJournalItem>, PackedSegmentError> {
        let index = self
            .open
            .iter()
            .position(|segment| segment.segment_id == segment_id)
            .ok_or_else(|| PackedSegmentError::Invalid {
                reason: format!(
                    "no open segment {segment_id} in erasure set {}",
                    self.erasure_set_id
                ),
            })?;
        self.seal_at(index, now, reason)
    }

    /// Take the sealed segments, handing them to the transcode pipeline.
    pub fn take_sealed(&mut self) -> Vec<PackedSegment> {
        std::mem::take(&mut self.sealed)
    }

    /// Take one sealed segment out of the pool, once it is durable.
    ///
    /// A sealed segment left in the pool is a copy of a record that the
    /// pipeline is about to move on: it gets encoded, then released. Handing
    /// that stale copy to a later seal would write it back over the record the
    /// pipeline produced, so the pool lets go of a segment as soon as somebody
    /// has made it durable.
    pub fn take_sealed_segment(&mut self, segment_id: PackedSegmentId) -> Option<PackedSegment> {
        let index = self.sealed.iter().position(|segment| segment.segment_id == segment_id)?;
        Some(self.sealed.remove(index))
    }

    fn seal_at(
        &mut self,
        index: usize,
        now: u64,
        reason: SealReason,
    ) -> Result<Vec<PackedSegmentJournalItem>, PackedSegmentError> {
        let mut segment = self.open.remove(index);
        let item = segment.journal_seal(now, reason);
        segment.seal(now, reason)?;
        self.sealed.push(segment);
        Ok(vec![item])
    }

    fn segment(&self, segment_id: PackedSegmentId) -> Result<&PackedSegment, PackedSegmentError> {
        self.open
            .iter()
            .find(|segment| segment.segment_id == segment_id)
            .ok_or_else(|| PackedSegmentError::ReservationMismatch {
                reason: format!("segment {segment_id} is not open in this pool"),
            })
    }

    fn segment_mut(
        &mut self,
        segment_id: PackedSegmentId,
    ) -> Result<&mut PackedSegment, PackedSegmentError> {
        self.open
            .iter_mut()
            .find(|segment| segment.segment_id == segment_id)
            .ok_or_else(|| PackedSegmentError::ReservationMismatch {
                reason: format!("segment {segment_id} is not open in this pool"),
            })
    }
}

/// Wall clock in nanoseconds since the Unix epoch.
pub fn unix_time_nanos() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    nanos.min(u128::from(u64::MAX)) as u64
}

fn group_by_segment(
    items: Vec<PackedSegmentJournalItem>,
) -> Vec<(PackedSegmentId, Vec<PackedSegmentJournalItem>)> {
    let mut grouped: Vec<(PackedSegmentId, Vec<PackedSegmentJournalItem>)> = Vec::new();
    for item in items {
        match grouped.iter_mut().find(|(segment_id, _)| *segment_id == item.segment_id) {
            Some((_, journal)) => journal.push(item),
            None => grouped.push((item.segment_id, vec![item])),
        }
    }
    grouped
}

type ReplayResult = Result<(PackedSegment, Option<PackedSegmentDiscardedTail>), String>;

fn replay_one_segment(
    segment_id: PackedSegmentId,
    journal: &[PackedSegmentJournalItem],
) -> ReplayResult {
    let mut items = journal.iter();
    let Some(first) = items.next() else {
        return Err("journal is empty".to_string());
    };
    let PackedSegmentJournalPayload::SegmentOpened {
        erasure_set_id,
        target_bytes,
        max_objects,
        created_at,
    } = &first.payload
    else {
        return Err(format!(
            "journal starts with {} instead of an open record",
            payload_name(&first.payload)
        ));
    };
    if first.sequence != 0 {
        return Err(format!(
            "open record carries sequence {} instead of 0",
            first.sequence
        ));
    }

    let mut segment = PackedSegment {
        segment_id,
        erasure_set_id: *erasure_set_id,
        state: PackedSegmentState::Open,
        target_bytes: *target_bytes,
        max_objects: *max_objects,
        used_bytes: 0,
        dead_bytes: 0,
        entries: Vec::new(),
        created_at: *created_at,
        sealed_at: None,
        seal_reason: None,
        next_sequence: 1,
        repacked_from: None,
        dead_at: None,
        pending: None,
    };

    let mut stop_reason: Option<String> = None;
    let mut replayed = 1usize;
    for item in items {
        if let Some(reason) = replay_step(&mut segment, item) {
            stop_reason = Some(reason);
            break;
        }
        replayed += 1;
    }

    let discarded = stop_reason.map(|reason| PackedSegmentDiscardedTail {
        segment_id,
        from_sequence: journal.get(replayed).map_or(segment.next_sequence, |item| item.sequence),
        discarded_records: journal.len().saturating_sub(replayed),
        reason,
    });
    Ok((segment, discarded))
}

/// Apply one journal record, or explain why replay must stop here.
fn replay_step(segment: &mut PackedSegment, item: &PackedSegmentJournalItem) -> Option<String> {
    if item.sequence != segment.next_sequence {
        return Some(format!(
            "record {} arrived where sequence {} was expected",
            item.sequence, segment.next_sequence
        ));
    }
    match &item.payload {
        PackedSegmentJournalPayload::Unreadable { reason } => {
            Some(format!("record {} is unreadable: {reason}", item.sequence))
        }
        PackedSegmentJournalPayload::SegmentOpened { .. } => Some(format!(
            "record {} opens a segment that is already open",
            item.sequence
        )),
        PackedSegmentJournalPayload::ObjectAppended { entry } => {
            if !segment.state.accepts_objects() {
                return Some(format!(
                    "record {} appends to a {} segment",
                    item.sequence, segment.state
                ));
            }
            segment
                .apply_append(entry.as_ref().clone())
                .err()
                .map(|error| format!("record {} is inconsistent: {error}", item.sequence))
        }
        PackedSegmentJournalPayload::SegmentSealed { sealed_at, reason } => segment
            .seal(*sealed_at, *reason)
            .err()
            .map(|error| format!("record {} cannot seal the segment: {error}", item.sequence)),
    }
}

fn payload_name(payload: &PackedSegmentJournalPayload) -> &'static str {
    match payload {
        PackedSegmentJournalPayload::SegmentOpened { .. } => "an open record",
        PackedSegmentJournalPayload::ObjectAppended { .. } => "an append record",
        PackedSegmentJournalPayload::SegmentSealed { .. } => "a seal record",
        PackedSegmentJournalPayload::Unreadable { .. } => "an unreadable record",
    }
}

fn parse_optional_u64_env(name: &'static str) -> Result<Option<u64>, EcConfigError> {
    match std::env::var(name) {
        Ok(raw) => raw.trim().parse::<u64>().map(Some).map_err(|error| EcConfigError::Env {
            name,
            reason: format!("'{raw}' is not a whole number: {error}"),
        }),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(EcConfigError::Env {
            name,
            reason: error.to_string(),
        }),
    }
}

fn parse_optional_u32_env(name: &'static str) -> Result<Option<u32>, EcConfigError> {
    match parse_optional_u64_env(name)? {
        Some(value) => u32::try_from(value).map(Some).map_err(|_| EcConfigError::Env {
            name,
            reason: format!("{value} does not fit in 32 bits"),
        }),
        None => Ok(None),
    }
}

fn ensure_range_u64(
    name: &'static str,
    value: u64,
    low: u64,
    high: u64,
) -> Result<(), EcConfigError> {
    if value < low || value > high {
        return Err(EcConfigError::Env {
            name,
            reason: format!("{value} is outside the allowed range {low}..={high}"),
        });
    }
    Ok(())
}

fn ensure_range_u32(
    name: &'static str,
    value: u32,
    low: u32,
    high: u32,
) -> Result<(), EcConfigError> {
    if value < low || value > high {
        return Err(EcConfigError::Env {
            name,
            reason: format!("{value} is outside the allowed range {low}..={high}"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use s4_cluster::HlcTimestamp;

    use super::*;
    use crate::ec_metadata::EcObjectVersionKey;
    use crate::erasure_coding::{EcConfigError, PLACEMENT_ENV_LOCK};

    struct PackedSegmentEnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl PackedSegmentEnvGuard {
        fn acquire() -> Self {
            let lock = PLACEMENT_ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            Self::clear();
            Self { _lock: lock }
        }

        fn clear() {
            std::env::remove_var(PACKED_SEGMENTS_ENABLED_ENV);
            std::env::remove_var(PACKED_SEGMENT_TARGET_BYTES_ENV);
            std::env::remove_var(PACKED_SEGMENT_MAX_AGE_SECS_ENV);
            std::env::remove_var(PACKED_SEGMENT_MAX_OBJECTS_ENV);
            std::env::remove_var(PACKED_SEGMENT_MAX_OPEN_ENV);
        }
    }

    impl Drop for PackedSegmentEnvGuard {
        fn drop(&mut self) {
            Self::clear();
        }
    }

    fn object(n: usize) -> EcObjectVersionKey {
        EcObjectVersionKey::new("test-bucket", format!("key-{n}"), None)
    }

    fn hash(n: u8) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = n;
        h
    }

    fn entry(
        object: EcObjectVersionKey,
        offset: u64,
        length: u64,
        hash: [u8; 32],
    ) -> PackedSegmentEntry {
        PackedSegmentEntry {
            object,
            offset,
            length,
            content_hash_sha256: hash,
            hlc: HlcTimestamp::zero(),
            etag: String::new(),
            content_type: String::new(),
            user_metadata: HashMap::new(),
        }
    }

    fn test_config(
        target_bytes: u64,
        max_objects: u32,
        max_open: u32,
        max_age_secs: u64,
    ) -> PackedSegmentConfig {
        PackedSegmentConfig {
            enabled: true,
            target_bytes,
            max_age: Duration::from_secs(max_age_secs),
            max_objects,
            max_open_segments: max_open,
        }
    }

    fn append_to_segment(
        segment: &mut PackedSegment,
        object: EcObjectVersionKey,
        length: u64,
        hash: [u8; 32],
        hlc: HlcTimestamp,
    ) -> Result<PackedSegmentEntry, PackedSegmentError> {
        let reservation = segment.reserve(length)?;
        let item =
            segment.journal_append(&reservation, PackedObjectIdentity::new(object, hash, hlc))?;
        segment.commit(&reservation, &item).cloned()
    }

    // -----------------------------------------------------------------------
    // 1. Configuration tests
    // -----------------------------------------------------------------------

    #[test]
    fn config_defaults_keep_packing_off() {
        let config = PackedSegmentConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.target_bytes, DEFAULT_PACKED_SEGMENT_TARGET_BYTES);
        assert_eq!(
            config.max_age,
            Duration::from_secs(DEFAULT_PACKED_SEGMENT_MAX_AGE_SECS)
        );
        assert_eq!(config.max_objects, DEFAULT_PACKED_SEGMENT_MAX_OBJECTS);
        assert_eq!(config.max_open_segments, DEFAULT_PACKED_SEGMENT_MAX_OPEN);
    }

    #[test]
    fn config_from_env_without_variables_matches_default() {
        let _guard = PackedSegmentEnvGuard::acquire();
        let config = PackedSegmentConfig::from_env().unwrap();
        assert_eq!(config, PackedSegmentConfig::default());
    }

    #[test]
    fn config_from_env_reads_every_variable() {
        let _guard = PackedSegmentEnvGuard::acquire();

        for bool_spelling in ["true", "1", "yes", "on"] {
            PackedSegmentEnvGuard::clear();
            std::env::set_var(PACKED_SEGMENTS_ENABLED_ENV, bool_spelling);
            std::env::set_var(PACKED_SEGMENT_TARGET_BYTES_ENV, "16777216");
            std::env::set_var(PACKED_SEGMENT_MAX_AGE_SECS_ENV, "120");
            std::env::set_var(PACKED_SEGMENT_MAX_OBJECTS_ENV, "1000");
            std::env::set_var(PACKED_SEGMENT_MAX_OPEN_ENV, "8");

            let config = PackedSegmentConfig::from_env().unwrap();
            assert!(config.enabled, "spelling '{bool_spelling}' should enable");
            assert_eq!(config.target_bytes, 16_777_216);
            assert_eq!(config.max_age, Duration::from_secs(120));
            assert_eq!(config.max_objects, 1000);
            assert_eq!(config.max_open_segments, 8);
        }
    }

    #[test]
    fn config_from_env_rejects_out_of_range_values() {
        let _guard = PackedSegmentEnvGuard::acquire();

        let cases = [
            (PACKED_SEGMENT_TARGET_BYTES_ENV, "0"),
            (
                PACKED_SEGMENT_TARGET_BYTES_ENV,
                &(MAX_PACKED_SEGMENT_TARGET_BYTES + 1).to_string(),
            ),
            (PACKED_SEGMENT_MAX_AGE_SECS_ENV, "0"),
            (
                PACKED_SEGMENT_MAX_AGE_SECS_ENV,
                &(MAX_PACKED_SEGMENT_MAX_AGE_SECS + 1).to_string(),
            ),
            (PACKED_SEGMENT_MAX_OBJECTS_ENV, "0"),
            (
                PACKED_SEGMENT_MAX_OBJECTS_ENV,
                &(MAX_PACKED_SEGMENT_MAX_OBJECTS + 1).to_string(),
            ),
            (PACKED_SEGMENT_MAX_OPEN_ENV, "0"),
            (
                PACKED_SEGMENT_MAX_OPEN_ENV,
                &(MAX_PACKED_SEGMENT_MAX_OPEN + 1).to_string(),
            ),
        ];

        for (var_name, val) in cases {
            PackedSegmentEnvGuard::clear();
            std::env::set_var(var_name, val);
            let err = PackedSegmentConfig::from_env().unwrap_err();
            match &err {
                EcConfigError::Env { name, .. } => {
                    assert_eq!(*name, var_name, "expected env error for {var_name}");
                }
                other => panic!("expected EcConfigError::Env for {var_name}, got {other:?}"),
            }
            assert!(
                err.to_string().contains(var_name),
                "error message '{err}' must contain variable name '{var_name}'"
            );
        }
    }

    #[test]
    fn config_from_env_rejects_non_numeric_value() {
        let _guard = PackedSegmentEnvGuard::acquire();
        std::env::set_var(PACKED_SEGMENT_TARGET_BYTES_ENV, "many");
        let err = PackedSegmentConfig::from_env().unwrap_err();
        match &err {
            EcConfigError::Env { name, .. } => {
                assert_eq!(*name, PACKED_SEGMENT_TARGET_BYTES_ENV);
            }
            other => panic!("expected EcConfigError::Env, got {other:?}"),
        }
        assert!(err.to_string().contains("many"));
    }

    #[test]
    fn config_validate_for_profile_rejects_segment_smaller_than_eight_objects() {
        let mut config = PackedSegmentConfig::default();
        let min_object_size = 1024 * 1024;

        config.target_bytes = 4 * 1024 * 1024;
        let err = config.validate_for_profile(min_object_size).unwrap_err();
        assert!(matches!(
            err,
            EcConfigError::Env {
                name: PACKED_SEGMENT_TARGET_BYTES_ENV,
                ..
            }
        ));

        config.target_bytes = 8 * 1024 * 1024;
        assert!(config.validate_for_profile(min_object_size).is_ok());
    }

    #[test]
    fn config_accepts_only_small_non_empty_objects() {
        let min_obj = 1024 * 1024;
        let mut config = PackedSegmentConfig {
            enabled: true,
            target_bytes: 64 * 1024 * 1024,
            ..PackedSegmentConfig::default()
        };

        assert!(!config.accepts_object_size(0, min_obj));
        assert!(config.accepts_object_size(1, min_obj));
        assert!(config.accepts_object_size(min_obj - 1, min_obj));
        assert!(!config.accepts_object_size(min_obj, min_obj));
        assert!(!config.accepts_object_size(config.target_bytes + 1, min_obj));

        config.enabled = false;
        assert!(!config.accepts_object_size(0, min_obj));
        assert!(!config.accepts_object_size(1, min_obj));
        assert!(!config.accepts_object_size(min_obj - 1, min_obj));
        assert!(!config.accepts_object_size(min_obj, min_obj));
        assert!(!config.accepts_object_size(config.target_bytes + 1, min_obj));
    }

    // -----------------------------------------------------------------------
    // 2. States and transitions tests
    // -----------------------------------------------------------------------

    #[test]
    fn state_open_is_the_only_one_accepting_objects() {
        assert!(PackedSegmentState::Open.accepts_objects());
        assert!(!PackedSegmentState::Sealed.accepts_objects());
        assert!(!PackedSegmentState::Encoded.accepts_objects());
        assert!(!PackedSegmentState::Purged.accepts_objects());
        assert!(!PackedSegmentState::Dead.accepts_objects());
    }

    #[test]
    fn state_transitions_follow_the_lifecycle() {
        let all = [
            PackedSegmentState::Open,
            PackedSegmentState::Sealed,
            PackedSegmentState::Encoded,
            PackedSegmentState::Purged,
            PackedSegmentState::Dead,
        ];

        for from in all {
            for to in all {
                let allowed = match (from, to) {
                    (PackedSegmentState::Open, PackedSegmentState::Sealed)
                    | (PackedSegmentState::Sealed, PackedSegmentState::Encoded)
                    | (PackedSegmentState::Encoded, PackedSegmentState::Purged) => true,
                    (PackedSegmentState::Dead, _) => false,
                    (_, PackedSegmentState::Dead) => true,
                    _ => false,
                };
                assert_eq!(
                    from.can_transition_to(to),
                    allowed,
                    "transition from {from} to {to} expected {allowed}"
                );
            }
        }
    }

    #[test]
    fn segment_refuses_illegal_transition_with_error() {
        let config = test_config(1000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);
        let err = segment.transition_to(PackedSegmentState::Encoded).unwrap_err();
        match err {
            PackedSegmentError::InvalidTransition {
                segment_id,
                from,
                to,
            } => {
                assert_eq!(segment_id, segment.segment_id);
                assert_eq!(from, PackedSegmentState::Open);
                assert_eq!(to, PackedSegmentState::Encoded);
            }
            other => panic!("expected InvalidTransition, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // 3. Offset and reservation arithmetic tests
    // -----------------------------------------------------------------------

    #[test]
    fn appends_are_contiguous_and_do_not_overlap() {
        let config = test_config(1000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);

        let l1 = 100u64;
        let l2 = 250u64;
        let l3 = 400u64;

        let e1 =
            append_to_segment(&mut segment, object(1), l1, hash(1), HlcTimestamp::zero()).unwrap();
        let e2 =
            append_to_segment(&mut segment, object(2), l2, hash(2), HlcTimestamp::zero()).unwrap();
        let e3 =
            append_to_segment(&mut segment, object(3), l3, hash(3), HlcTimestamp::zero()).unwrap();

        assert_eq!(e1.offset, 0);
        assert_eq!(e1.length, l1);
        assert_eq!(e2.offset, l1);
        assert_eq!(e2.length, l2);
        assert_eq!(e3.offset, l1 + l2);
        assert_eq!(e3.length, l3);

        assert_eq!(segment.used_bytes, l1 + l2 + l3);
        assert_eq!(segment.live_bytes(), l1 + l2 + l3);
        assert_eq!(segment.dead_ratio(), 0.0);
        assert!(segment.validate().is_ok());
    }

    #[test]
    fn object_that_exactly_fills_the_segment_is_accepted() {
        let config = test_config(100, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);

        let entry =
            append_to_segment(&mut segment, object(1), 100, hash(1), HlcTimestamp::zero()).unwrap();
        assert_eq!(entry.offset, 0);
        assert_eq!(entry.length, 100);
        assert_eq!(segment.remaining_bytes(), 0);
        assert_eq!(segment.used_bytes, 100);
        assert!(segment.validate().is_ok());
    }

    #[test]
    fn object_that_exactly_does_not_fit_is_refused() {
        let config = test_config(100, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);
        append_to_segment(&mut segment, object(1), 60, hash(1), HlcTimestamp::zero()).unwrap();
        assert_eq!(segment.remaining_bytes(), 40);

        let err = segment.reserve(41).unwrap_err();
        match err {
            PackedSegmentError::SegmentFull {
                segment_id,
                remaining,
                length,
            } => {
                assert_eq!(segment_id, segment.segment_id);
                assert_eq!(remaining, 40);
                assert_eq!(length, 41);
            }
            other => panic!("expected SegmentFull, got {other:?}"),
        }

        let reservation = segment.reserve(40).unwrap();
        assert_eq!(reservation.offset, 60);
        assert_eq!(reservation.length, 40);
    }

    #[test]
    fn object_larger_than_the_segment_is_never_packed() {
        let config = test_config(100, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);

        let err_too_large = segment.reserve(101).unwrap_err();
        assert!(matches!(
            err_too_large,
            PackedSegmentError::ObjectTooLarge {
                length: 101,
                target_bytes: 100,
            }
        ));

        let err_zero = segment.reserve(0).unwrap_err();
        assert!(matches!(
            err_zero,
            PackedSegmentError::ObjectTooLarge {
                length: 0,
                target_bytes: 100,
            }
        ));
    }

    #[test]
    fn reservation_is_not_visible_until_committed() {
        let config = test_config(100, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);

        let _reservation = segment.reserve(30).unwrap();
        assert_eq!(segment.used_bytes, 0);
        assert!(segment.entries.is_empty());
        assert!(segment.has_pending_reservation());
        assert_eq!(segment.remaining_bytes(), 70);
    }

    #[test]
    fn aborted_reservation_rolls_the_cursor_back_exactly() {
        let config = test_config(100, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);

        let reservation = segment.reserve(30).unwrap();
        segment.abort(&reservation).unwrap();

        assert_eq!(segment.used_bytes, 0);
        assert_eq!(segment.remaining_bytes(), 100);
        assert!(!segment.has_pending_reservation());

        let next_res = segment.reserve(50).unwrap();
        assert_eq!(next_res.offset, 0);
        assert_eq!(next_res.length, 50);
    }

    #[test]
    fn second_reservation_on_one_segment_is_refused() {
        let config = test_config(100, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);

        let _r1 = segment.reserve(10).unwrap();
        let err = segment.reserve(10).unwrap_err();
        assert!(matches!(
            err,
            PackedSegmentError::ReservationInFlight { segment_id } if segment_id == segment.segment_id
        ));
    }

    #[test]
    fn commit_rejects_a_journal_item_from_another_reservation() {
        let config = test_config(100, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);
        let r = segment.reserve(20).unwrap();
        let item = segment
            .journal_append(
                &r,
                PackedObjectIdentity::new(object(1), hash(1), HlcTimestamp::zero()),
            )
            .unwrap();

        // Wrong sequence
        let mut wrong_seq = item.clone();
        wrong_seq.sequence += 1;
        assert!(matches!(
            segment.commit(&r, &wrong_seq),
            Err(PackedSegmentError::ReservationMismatch { .. })
        ));

        // Wrong offset
        let mut wrong_offset = item.clone();
        if let PackedSegmentJournalPayload::ObjectAppended { ref mut entry } = wrong_offset.payload
        {
            entry.offset += 1;
        }
        assert!(matches!(
            segment.commit(&r, &wrong_offset),
            Err(PackedSegmentError::ReservationMismatch { .. })
        ));
    }

    #[test]
    fn abort_without_reservation_is_refused() {
        let config = test_config(100, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);

        let dummy = PackedSegmentReservation {
            segment_id: segment.segment_id,
            offset: 0,
            length: 10,
            sequence: 1,
        };
        assert!(matches!(
            segment.abort(&dummy),
            Err(PackedSegmentError::ReservationMismatch { .. })
        ));
    }

    #[test]
    fn sealed_segment_refuses_new_objects() {
        let config = test_config(100, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);
        segment.seal(1000, SealReason::OperatorRequest).unwrap();

        let err = segment.reserve(10).unwrap_err();
        assert!(matches!(
            err,
            PackedSegmentError::NotOpen {
                segment_id,
                state: PackedSegmentState::Sealed,
            } if segment_id == segment.segment_id
        ));
    }

    #[test]
    fn entry_validate_rejects_zero_length_and_bad_key() {
        let zero_len = entry(object(1), 0, 0, hash(1));
        assert!(matches!(
            zero_len.validate(),
            Err(PackedSegmentError::Invalid { .. })
        ));

        let empty_bucket = entry(EcObjectVersionKey::new("", "key", None), 0, 10, hash(1));
        assert!(matches!(
            empty_bucket.validate(),
            Err(PackedSegmentError::Invalid { .. })
        ));

        let empty_key = entry(EcObjectVersionKey::new("bucket", "", None), 0, 10, hash(1));
        assert!(matches!(
            empty_key.validate(),
            Err(PackedSegmentError::Invalid { .. })
        ));

        let empty_version = entry(
            EcObjectVersionKey::new("bucket", "key", Some("".to_string())),
            0,
            10,
            hash(1),
        );
        assert!(matches!(
            empty_version.validate(),
            Err(PackedSegmentError::Invalid { .. })
        ));

        let valid = entry(object(1), 0, 10, hash(1));
        assert!(valid.validate().is_ok());
    }

    #[test]
    fn segment_validate_catches_broken_accounting() {
        let config = test_config(100, 10, 2, 300);
        let base = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);

        // Accounting mismatch: sum(entries) + dead_bytes != used_bytes
        let mut broken_accounting = base.clone();
        broken_accounting.used_bytes = 20;
        assert!(matches!(
            broken_accounting.validate(),
            Err(PackedSegmentError::Invalid { .. })
        ));

        // Overlapping entries
        let mut broken_overlap = base.clone();
        let e1 = entry(object(1), 0, 10, hash(1));
        let e2 = entry(object(2), 5, 10, hash(2));
        broken_overlap.entries = vec![e1, e2];
        broken_overlap.used_bytes = 15;
        assert!(matches!(
            broken_overlap.validate(),
            Err(PackedSegmentError::Invalid { .. })
        ));

        // Sealed segment without sealed_at
        let mut broken_sealed = base.clone();
        broken_sealed.state = PackedSegmentState::Sealed;
        broken_sealed.sealed_at = None;
        broken_sealed.seal_reason = Some(SealReason::TargetSizeReached);
        assert!(matches!(
            broken_sealed.validate(),
            Err(PackedSegmentError::Invalid { .. })
        ));

        // Open segment carrying a seal record
        let mut broken_open = base.clone();
        broken_open.sealed_at = Some(100);
        broken_open.seal_reason = Some(SealReason::TargetSizeReached);
        assert!(matches!(
            broken_open.validate(),
            Err(PackedSegmentError::Invalid { .. })
        ));
    }

    // -----------------------------------------------------------------------
    // 4. Sealing condition tests
    // -----------------------------------------------------------------------

    #[test]
    fn segment_seals_when_max_age_is_reached() {
        let config = test_config(1000, 10, 2, 5);
        let max_age_nanos = config.max_age_nanos();
        let created_at = 1_000_000_000u64;
        let segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, created_at);

        // Before max age
        assert_eq!(
            segment.seal_condition(created_at + max_age_nanos - 1, &config),
            None
        );
        // Exactly at max age
        assert_eq!(
            segment.seal_condition(created_at + max_age_nanos, &config),
            Some(SealReason::MaxAgeReached)
        );
        // After max age
        assert_eq!(
            segment.seal_condition(created_at + max_age_nanos + 1_000_000, &config),
            Some(SealReason::MaxAgeReached)
        );
    }

    #[test]
    fn segment_seals_when_max_objects_is_reached() {
        let config = test_config(1000, 2, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);
        assert_eq!(segment.seal_condition(0, &config), None);

        append_to_segment(&mut segment, object(1), 10, hash(1), HlcTimestamp::zero()).unwrap();
        assert_eq!(segment.seal_condition(0, &config), None);

        append_to_segment(&mut segment, object(2), 10, hash(2), HlcTimestamp::zero()).unwrap();
        assert_eq!(
            segment.seal_condition(0, &config),
            Some(SealReason::MaxObjectsReached)
        );
    }

    #[test]
    fn seal_records_reason_and_time() {
        let config = test_config(1000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);
        let seq_before = segment.next_sequence;
        let sealed_at = 5_000_000_000u64;

        segment.seal(sealed_at, SealReason::TargetSizeReached).unwrap();

        assert_eq!(segment.state, PackedSegmentState::Sealed);
        assert_eq!(segment.sealed_at, Some(sealed_at));
        assert_eq!(segment.seal_reason, Some(SealReason::TargetSizeReached));
        assert_eq!(segment.next_sequence, seq_before + 1);
        assert!(segment.validate().is_ok());
    }

    #[test]
    fn sealing_drops_a_pending_reservation() {
        let config = test_config(1000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 0);
        let _r = segment.reserve(50).unwrap();
        assert!(segment.has_pending_reservation());

        segment.seal(1000, SealReason::OperatorRequest).unwrap();
        assert!(!segment.has_pending_reservation());
    }

    // -----------------------------------------------------------------------
    // 5. Pool tests
    // -----------------------------------------------------------------------

    #[test]
    fn pool_opens_a_new_segment_when_the_object_does_not_fit() {
        let config = test_config(100, 10, 4, 300);
        let mut pool = PackedSegmentPool::new(1, config);

        // First object: 60 bytes
        let adm1 = pool.reserve(60, 100).unwrap();
        let item1 = pool
            .journal_append(
                &adm1.reservation,
                PackedObjectIdentity::new(object(1), hash(1), HlcTimestamp::zero()),
            )
            .unwrap();
        let entry1 = pool.commit(&adm1.reservation, &item1).unwrap();
        let first_seg_id = adm1.reservation.segment_id;

        assert_eq!(entry1.offset, 0);
        assert_eq!(entry1.length, 60);

        // Second object: 50 bytes (does not fit into remaining 40 bytes of first segment)
        let adm2 = pool.reserve(50, 101).unwrap();
        assert_ne!(adm2.reservation.segment_id, first_seg_id);
        assert_eq!(adm2.reservation.offset, 0);
        assert_eq!(adm2.reservation.length, 50);

        let has_seal = adm2.prerequisites.iter().any(|item| {
            item.segment_id == first_seg_id
                && matches!(
                    item.payload,
                    PackedSegmentJournalPayload::SegmentSealed {
                        reason: SealReason::TargetSizeReached,
                        ..
                    }
                )
        });
        let has_open = adm2.prerequisites.iter().any(|item| {
            item.segment_id == adm2.reservation.segment_id
                && matches!(
                    item.payload,
                    PackedSegmentJournalPayload::SegmentOpened { .. }
                )
        });
        assert!(
            has_seal,
            "prerequisites must contain seal record for first segment"
        );
        assert!(
            has_open,
            "prerequisites must contain open record for new segment"
        );

        let item2 = pool
            .journal_append(
                &adm2.reservation,
                PackedObjectIdentity::new(object(2), hash(2), HlcTimestamp::zero()),
            )
            .unwrap();
        let entry2 = pool.commit(&adm2.reservation, &item2).unwrap();
        assert_eq!(entry2.offset, 0);
        assert_eq!(entry2.length, 50);

        assert_eq!(pool.sealed_segments().len(), 1);
        assert_eq!(pool.sealed_segments()[0].entries.len(), 1);
        assert_eq!(pool.sealed_segments()[0].entries[0], entry1);

        assert_eq!(pool.open_segments().len(), 1);
        assert_eq!(pool.open_segments()[0].entries.len(), 1);
        assert_eq!(pool.open_segments()[0].entries[0], entry2);
    }

    #[test]
    fn pool_seals_expired_segments_on_reserve_and_on_demand() {
        let config = test_config(1000, 10, 4, 10);
        let mut pool = PackedSegmentPool::new(1, config);
        let created_at = 1_000_000_000u64;

        let adm = pool.reserve(100, created_at).unwrap();
        let item = pool
            .journal_append(
                &adm.reservation,
                PackedObjectIdentity::new(object(1), hash(1), HlcTimestamp::zero()),
            )
            .unwrap();
        pool.commit(&adm.reservation, &item).unwrap();

        assert_eq!(pool.open_segments().len(), 1);
        assert_eq!(pool.sealed_segments().len(), 0);

        let items_unexpired = pool.seal_expired(created_at + 5_000_000_000);
        assert!(items_unexpired.is_empty());
        assert_eq!(pool.open_segments().len(), 1);

        let items_expired = pool.seal_expired(created_at + 10_000_000_000);
        assert_eq!(items_expired.len(), 1);
        assert!(matches!(
            items_expired[0].payload,
            PackedSegmentJournalPayload::SegmentSealed {
                reason: SealReason::MaxAgeReached,
                ..
            }
        ));
        assert_eq!(pool.open_segments().len(), 0);
        assert_eq!(pool.sealed_segments().len(), 1);

        let taken = pool.take_sealed();
        assert_eq!(taken.len(), 1);
        assert_eq!(pool.sealed_segments().len(), 0);
    }

    #[test]
    fn pool_spreads_concurrent_appends_over_open_segments() {
        let config = test_config(1000, 10, 3, 300);
        let mut pool = PackedSegmentPool::new(1, config);

        let adm1 = pool.reserve(100, 100).unwrap();
        let adm2 = pool.reserve(100, 100).unwrap();
        let adm3 = pool.reserve(100, 100).unwrap();

        let mut ids = vec![
            adm1.reservation.segment_id,
            adm2.reservation.segment_id,
            adm3.reservation.segment_id,
        ];
        ids.sort();
        ids.dedup();
        assert_eq!(
            ids.len(),
            3,
            "each reservation must be assigned to a distinct open segment"
        );

        let err = pool.reserve(100, 100).unwrap_err();
        assert!(matches!(err, PackedSegmentError::PoolExhausted { open: 3 }));
    }

    #[test]
    fn pool_seal_now_requires_an_open_segment() {
        let config = test_config(1000, 10, 2, 300);
        let mut pool = PackedSegmentPool::new(1, config);

        let err = pool
            .seal_now(
                PackedSegmentId::new_random(),
                100,
                SealReason::OperatorRequest,
            )
            .unwrap_err();
        assert!(matches!(err, PackedSegmentError::Invalid { .. }));
    }

    #[test]
    fn pool_from_recovered_rejects_foreign_erasure_set() {
        let config = test_config(1000, 10, 2, 300);
        let seg = PackedSegment::open(PackedSegmentId::new_random(), 999, &config, 0);

        let err = PackedSegmentPool::from_recovered(42, config, vec![seg]).unwrap_err();
        assert!(matches!(err, PackedSegmentError::Invalid { .. }));
    }

    #[test]
    fn pool_from_recovered_splits_open_and_sealed() {
        let config = test_config(1000, 10, 2, 300);
        let open_seg = PackedSegment::open(PackedSegmentId::new_random(), 42, &config, 0);
        let mut sealed_seg = PackedSegment::open(PackedSegmentId::new_random(), 42, &config, 0);
        sealed_seg.seal(1000, SealReason::OperatorRequest).unwrap();

        let pool =
            PackedSegmentPool::from_recovered(42, config, vec![open_seg, sealed_seg]).unwrap();
        assert_eq!(pool.open_segments().len(), 1);
        assert_eq!(pool.sealed_segments().len(), 1);
    }

    // -----------------------------------------------------------------------
    // 6. Recovery tests
    // -----------------------------------------------------------------------

    #[test]
    fn recovery_restores_every_acknowledged_object() {
        let seg_id = PackedSegmentId::new_random();
        let config = test_config(1000, 10, 2, 300);
        let seg = PackedSegment::open(seg_id, 1, &config, 100);
        let mut items = vec![seg.journal_open()];

        let lengths = [50u64, 100u64, 150u64];
        let mut offset = 0u64;
        for (i, &length) in lengths.iter().enumerate() {
            let entry = entry(object(i + 1), offset, length, hash((i + 1) as u8));
            offset += length;
            items.push(PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: (i + 1) as u64,
                payload: PackedSegmentJournalPayload::ObjectAppended {
                    entry: Box::new(entry),
                },
            });
        }

        let recovery = recover_segments(items);
        assert_eq!(recovery.segments.len(), 1);
        assert!(recovery.discarded.is_empty());
        assert!(recovery.rejected.is_empty());

        let recovered = &recovery.segments[0];
        assert_eq!(recovered.used_bytes, 300);
        assert_eq!(recovered.entries.len(), 3);
        assert_eq!(recovered.entries[0].offset, 0);
        assert_eq!(recovered.entries[1].offset, 50);
        assert_eq!(recovered.entries[2].offset, 150);
        assert_eq!(recovered.next_sequence, 4);
        assert!(recovered.validate().is_ok());
    }

    #[test]
    fn recovery_stops_at_the_first_unreadable_record() {
        let seg_id = PackedSegmentId::new_random();
        let config = test_config(1000, 10, 2, 300);
        let seg = PackedSegment::open(seg_id, 1, &config, 100);

        let entry1 = entry(object(1), 0, 100, hash(1));
        let entry2 = entry(object(2), 100, 100, hash(2));

        let items = vec![
            seg.journal_open(),
            PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: 1,
                payload: PackedSegmentJournalPayload::ObjectAppended {
                    entry: Box::new(entry1),
                },
            },
            PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: 2,
                payload: PackedSegmentJournalPayload::Unreadable {
                    reason: "crc mismatch".to_string(),
                },
            },
            PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: 3,
                payload: PackedSegmentJournalPayload::ObjectAppended {
                    entry: Box::new(entry2),
                },
            },
        ];

        let recovery = recover_segments(items);
        assert_eq!(recovery.segments.len(), 1);
        assert_eq!(recovery.segments[0].entries.len(), 1);
        assert_eq!(recovery.segments[0].entries[0].object, object(1));
        assert_eq!(recovery.discarded.len(), 1);
        assert_eq!(recovery.discarded[0].from_sequence, 2);
        assert_eq!(recovery.discarded[0].discarded_records, 2);
        assert!(recovery.rejected.is_empty());
    }

    #[test]
    fn recovery_stops_at_a_sequence_gap() {
        let seg_id = PackedSegmentId::new_random();
        let config = test_config(1000, 10, 2, 300);
        let seg = PackedSegment::open(seg_id, 1, &config, 100);

        let entry1 = entry(object(1), 0, 100, hash(1));
        let entry2 = entry(object(2), 100, 100, hash(2));

        let items = vec![
            seg.journal_open(),
            PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: 1,
                payload: PackedSegmentJournalPayload::ObjectAppended {
                    entry: Box::new(entry1),
                },
            },
            // sequence 2 is missing
            PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: 3,
                payload: PackedSegmentJournalPayload::ObjectAppended {
                    entry: Box::new(entry2),
                },
            },
        ];

        let recovery = recover_segments(items);
        assert_eq!(recovery.segments.len(), 1);
        assert_eq!(recovery.segments[0].entries.len(), 1);
        assert_eq!(recovery.discarded.len(), 1);
        assert_eq!(recovery.discarded[0].from_sequence, 3);
        assert_eq!(recovery.discarded[0].discarded_records, 1);
    }

    #[test]
    fn recovery_discards_records_after_seal() {
        let seg_id = PackedSegmentId::new_random();
        let config = test_config(1000, 10, 2, 300);
        let seg = PackedSegment::open(seg_id, 1, &config, 100);

        let entry1 = entry(object(1), 0, 100, hash(1));
        let entry2 = entry(object(2), 100, 100, hash(2));

        let items = vec![
            seg.journal_open(),
            PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: 1,
                payload: PackedSegmentJournalPayload::ObjectAppended {
                    entry: Box::new(entry1),
                },
            },
            PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: 2,
                payload: PackedSegmentJournalPayload::SegmentSealed {
                    sealed_at: 200,
                    reason: SealReason::OperatorRequest,
                },
            },
            PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: 3,
                payload: PackedSegmentJournalPayload::ObjectAppended {
                    entry: Box::new(entry2),
                },
            },
        ];

        let recovery = recover_segments(items);
        assert_eq!(recovery.segments.len(), 1);
        assert_eq!(recovery.segments[0].state, PackedSegmentState::Sealed);
        assert_eq!(recovery.segments[0].entries.len(), 1);
        assert_eq!(recovery.discarded.len(), 1);
        assert_eq!(recovery.discarded[0].from_sequence, 3);
        assert_eq!(recovery.discarded[0].discarded_records, 1);
    }

    #[test]
    fn recovered_segment_accepts_dead_set_entries() {
        let seg_id = PackedSegmentId::new_random();
        let config = test_config(1000, 10, 2, 300);
        let seg = PackedSegment::open(seg_id, 1, &config, 100);

        let entry1 = entry(object(1), 0, 100, hash(1));
        let entry2 = entry(object(2), 100, 200, hash(2));

        let items = vec![
            seg.journal_open(),
            PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: 1,
                payload: PackedSegmentJournalPayload::ObjectAppended {
                    entry: Box::new(entry1),
                },
            },
            PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: 2,
                payload: PackedSegmentJournalPayload::ObjectAppended {
                    entry: Box::new(entry2),
                },
            },
        ];

        let recovery = recover_segments(items);
        assert_eq!(recovery.segments.len(), 1);
        // Deletions are not journal records: they are an idempotent set applied
        // on top of the replayed segment, which is what lets a delete land on a
        // node that does not own the open segment.
        let mut recovered = recovery.segments[0].clone();
        assert_eq!(recovered.record_object_deleted(&object(1)).unwrap(), 100);
        assert!(recovered.record_object_deleted(&object(1)).is_err());
        let recovered = &recovered;
        assert_eq!(recovered.dead_bytes, 100);
        assert_eq!(recovered.used_bytes, 300);
        assert_eq!(recovered.live_bytes(), 200);
        assert_eq!(recovered.entries.len(), 1);
        assert_eq!(recovered.entries[0].object, object(2));
        assert!(recovered.validate().is_ok());
    }

    #[test]
    fn recovery_rejects_a_journal_that_does_not_start_with_open() {
        let seg_id = PackedSegmentId::new_random();
        let entry = entry(object(1), 0, 100, hash(1));
        let items = vec![PackedSegmentJournalItem {
            segment_id: seg_id,
            sequence: 1,
            payload: PackedSegmentJournalPayload::ObjectAppended {
                entry: Box::new(entry),
            },
        }];

        let recovery = recover_segments(items);
        assert!(recovery.segments.is_empty());
        assert_eq!(recovery.rejected.len(), 1);
        assert_eq!(recovery.rejected[0].segment_id, seg_id);
        assert!(recovery.rejected[0].reason.contains("starts with"));
    }

    #[test]
    fn recovery_rejects_an_open_record_with_a_non_zero_sequence() {
        let seg_id = PackedSegmentId::new_random();
        let items = vec![PackedSegmentJournalItem {
            segment_id: seg_id,
            sequence: 1,
            payload: PackedSegmentJournalPayload::SegmentOpened {
                erasure_set_id: 1,
                target_bytes: 1000,
                max_objects: 10,
                created_at: 100,
            },
        }];

        let recovery = recover_segments(items);
        assert!(recovery.segments.is_empty());
        assert_eq!(recovery.rejected.len(), 1);
        assert_eq!(recovery.rejected[0].segment_id, seg_id);
        assert!(recovery.rejected[0].reason.contains("sequence 1 instead of 0"));
    }

    #[test]
    fn recovery_handles_several_segments_independently() {
        let seg_a = PackedSegmentId::new_random();
        let seg_b = PackedSegmentId::new_random();
        let config = test_config(1000, 10, 2, 300);

        let open_a = PackedSegment::open(seg_a, 1, &config, 100).journal_open();
        let app_a1 = PackedSegmentJournalItem {
            segment_id: seg_a,
            sequence: 1,
            payload: PackedSegmentJournalPayload::ObjectAppended {
                entry: Box::new(entry(object(1), 0, 100, hash(1))),
            },
        };
        let unreadable_a2 = PackedSegmentJournalItem {
            segment_id: seg_a,
            sequence: 2,
            payload: PackedSegmentJournalPayload::Unreadable {
                reason: "corrupted block".to_string(),
            },
        };
        let app_a3 = PackedSegmentJournalItem {
            segment_id: seg_a,
            sequence: 3,
            payload: PackedSegmentJournalPayload::ObjectAppended {
                entry: Box::new(entry(object(2), 100, 100, hash(2))),
            },
        };

        let open_b = PackedSegment::open(seg_b, 1, &config, 100).journal_open();
        let app_b1 = PackedSegmentJournalItem {
            segment_id: seg_b,
            sequence: 1,
            payload: PackedSegmentJournalPayload::ObjectAppended {
                entry: Box::new(entry(object(3), 0, 150, hash(3))),
            },
        };
        let app_b2 = PackedSegmentJournalItem {
            segment_id: seg_b,
            sequence: 2,
            payload: PackedSegmentJournalPayload::ObjectAppended {
                entry: Box::new(entry(object(4), 150, 200, hash(4))),
            },
        };

        // Scramble items
        let items = vec![
            app_b2,
            open_a,
            app_a3,
            open_b,
            unreadable_a2,
            app_b1,
            app_a1,
        ];

        let recovery = recover_segments(items);
        assert_eq!(recovery.segments.len(), 2);
        assert_eq!(recovery.discarded.len(), 1);
        assert_eq!(recovery.discarded[0].segment_id, seg_a);
        assert_eq!(recovery.discarded[0].from_sequence, 2);

        let recovered_a = recovery.segments.iter().find(|s| s.segment_id == seg_a).unwrap();
        assert_eq!(recovered_a.entries.len(), 1);
        assert!(recovered_a.validate().is_ok());

        let recovered_b = recovery.segments.iter().find(|s| s.segment_id == seg_b).unwrap();
        assert_eq!(recovered_b.entries.len(), 2);
        assert!(recovered_b.validate().is_ok());
    }

    #[test]
    fn recovery_truncated_at_every_prefix_never_loses_an_acknowledged_object() {
        let seg_id = PackedSegmentId::new_random();
        let config = test_config(10_000, 10, 2, 300);
        let seg = PackedSegment::open(seg_id, 1, &config, 100);
        let mut items = vec![seg.journal_open()];

        let mut offset = 0u64;
        for i in 1..=5 {
            let len = 100;
            let entry = entry(object(i), offset, len, hash(i as u8));
            offset += len;
            items.push(PackedSegmentJournalItem {
                segment_id: seg_id,
                sequence: i as u64,
                payload: PackedSegmentJournalPayload::ObjectAppended {
                    entry: Box::new(entry),
                },
            });
        }

        for k in 1..=items.len() {
            let prefix = items[..k].to_vec();
            let recovery = recover_segments(prefix);
            assert_eq!(recovery.segments.len(), 1);
            assert!(recovery.rejected.is_empty());
            let recovered = &recovery.segments[0];
            let expected_entries = k - 1;
            assert_eq!(recovered.entries.len(), expected_entries);
            for idx in 0..expected_entries {
                assert_eq!(recovered.entries[idx].object, object(idx + 1));
            }
            assert!(recovered.validate().is_ok());
        }
    }

    #[test]
    fn manifest_key_round_trips_through_the_segment_id() {
        for _ in 0..10 {
            let id = PackedSegmentId::new_random();
            let key = packed_segment_manifest_key(id);
            assert_eq!(key.bucket, PACKED_SEGMENT_MANIFEST_BUCKET);
            assert_eq!(key.key, id.to_string());
            assert!(key.version_id.is_none());

            let roundtrip = packed_segment_id_from_manifest_key(&key)
                .expect("manifest key should resolve back to segment id");
            assert_eq!(roundtrip, id);
        }
    }

    #[test]
    fn a_key_outside_the_reserved_bucket_is_not_a_segment() {
        let id = PackedSegmentId::new_random();
        let key = EcObjectVersionKey::new("regular-bucket", id.to_string(), None);
        let err = packed_segment_id_from_manifest_key(&key).unwrap_err();
        match err {
            PackedSegmentError::Invalid { reason } => {
                assert!(
                    reason.contains(PACKED_SEGMENT_MANIFEST_BUCKET),
                    "error message '{reason}' must mention reserved bucket '{PACKED_SEGMENT_MANIFEST_BUCKET}'"
                );
            }
            other => panic!("expected PackedSegmentError::Invalid, got {other:?}"),
        }
    }

    #[test]
    fn a_key_whose_name_is_not_a_segment_id_is_rejected() {
        let key = EcObjectVersionKey::new(PACKED_SEGMENT_MANIFEST_BUCKET, "not-a-uuid", None);
        let err = packed_segment_id_from_manifest_key(&key).unwrap_err();
        assert!(matches!(err, PackedSegmentError::Invalid { .. }));
    }

    #[test]
    fn retiring_a_segment_records_when_it_died() {
        let config = test_config(10_000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 100);
        segment
            .append_without_journal(
                PackedObjectIdentity::new(object(1), hash(1), HlcTimestamp::zero()),
                500,
            )
            .unwrap();
        segment.seal(200, SealReason::TargetSizeReached).unwrap();
        segment.transition_to(PackedSegmentState::Encoded).unwrap();
        segment.transition_to(PackedSegmentState::Purged).unwrap();

        let now = 1_000;
        segment.retire(now).unwrap();

        assert_eq!(segment.state, PackedSegmentState::Dead);
        assert_eq!(segment.dead_at, Some(now));
        assert!(segment.validate().is_ok());
    }

    #[test]
    fn a_dead_segment_cannot_be_retired_twice() {
        let config = test_config(10_000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 100);
        segment.seal(200, SealReason::TargetSizeReached).unwrap();
        segment.retire(1_000).unwrap();

        let err = segment.retire(2_000).unwrap_err();
        assert!(matches!(err, PackedSegmentError::InvalidTransition { .. }));
    }

    #[test]
    fn a_segment_is_drained_only_after_its_grace_elapsed() {
        let config = test_config(10_000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 100);
        segment.seal(200, SealReason::TargetSizeReached).unwrap();
        let dead_at = 1_000_000;
        segment.retire(dead_at).unwrap();
        let grace = 60_000;

        assert!(!segment.is_drained(dead_at + grace - 1, grace));
        assert!(segment.is_drained(dead_at + grace, grace));
    }

    #[test]
    fn a_live_segment_is_never_drained() {
        let config = test_config(10_000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 100);
        assert!(!segment.is_drained(500_000, 0));

        segment.seal(200, SealReason::TargetSizeReached).unwrap();
        assert!(!segment.is_drained(500_000, 0));

        segment.transition_to(PackedSegmentState::Encoded).unwrap();
        assert!(!segment.is_drained(500_000, 0));

        segment.transition_to(PackedSegmentState::Purged).unwrap();
        assert!(!segment.is_drained(500_000, 0));
    }

    #[test]
    fn a_retirement_time_belongs_only_to_a_dead_segment() {
        let config = test_config(10_000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 100);
        segment.seal(200, SealReason::TargetSizeReached).unwrap();

        // A sealed segment that somehow carries a retirement time.
        segment.dead_at = Some(1);
        assert!(matches!(
            segment.validate(),
            Err(PackedSegmentError::Invalid { .. })
        ));

        // A dead segment that carries none.
        segment.state = PackedSegmentState::Dead;
        segment.dead_at = None;
        assert!(matches!(
            segment.validate(),
            Err(PackedSegmentError::Invalid { .. })
        ));
    }

    #[test]
    fn a_repack_replacement_is_sized_for_what_its_source_holds() {
        let source_config = test_config(64 * 1024 * 1024, 100, 2, 300);
        let mut source =
            PackedSegment::open(PackedSegmentId::new_random(), 42, &source_config, 100);
        for index in 1..=5 {
            source
                .append_without_journal(
                    PackedObjectIdentity::new(
                        object(index),
                        hash(index as u8),
                        HlcTimestamp::zero(),
                    ),
                    10_000,
                )
                .unwrap();
        }
        assert_eq!(source.live_bytes(), 50_000);
        assert_eq!(source.entries.len(), 5);

        let small_config = test_config(1_000, 2, 2, 300);
        let replacement =
            PackedSegment::open_repack(PackedSegmentId::new_random(), &source, &small_config, 200);

        assert!(replacement.target_bytes >= source.live_bytes());
        assert!(replacement.max_objects >= source.entries.len() as u32);
        assert_eq!(replacement.repacked_from, Some(source.segment_id));
        assert_eq!(replacement.erasure_set_id, source.erasure_set_id);
    }

    #[test]
    fn a_repack_replacement_packs_its_objects_without_holes() {
        let source_config = test_config(64 * 1024 * 1024, 10, 2, 300);
        let mut source = PackedSegment::open(PackedSegmentId::new_random(), 1, &source_config, 100);
        source
            .append_without_journal(
                PackedObjectIdentity::new(object(1), hash(1), HlcTimestamp::zero()),
                3_000,
            )
            .unwrap();

        let config = test_config(10_000, 10, 2, 300);
        let mut repacked =
            PackedSegment::open_repack(PackedSegmentId::new_random(), &source, &config, 200);

        let lengths = [100u64, 250, 400];
        let mut expected_offset = 0u64;
        for (index, &length) in lengths.iter().enumerate() {
            let entry = repacked
                .append_without_journal(
                    PackedObjectIdentity::new(
                        object(index + 1),
                        hash((index + 1) as u8),
                        HlcTimestamp::zero(),
                    ),
                    length,
                )
                .unwrap();
            assert_eq!(
                entry.offset, expected_offset,
                "the replacement must hold no holes"
            );
            expected_offset += length;
        }

        assert_eq!(repacked.used_bytes, lengths.iter().sum::<u64>());
        assert_eq!(repacked.dead_bytes, 0);
        assert!(repacked.validate().is_ok());
    }

    #[test]
    fn an_append_that_is_refused_leaves_no_reservation_behind() {
        let config = test_config(10_000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 100);

        let invalid_key = EcObjectVersionKey::new("", "key-1", None);
        let invalid_identity =
            PackedObjectIdentity::new(invalid_key, hash(1), HlcTimestamp::zero());
        let err = segment.append_without_journal(invalid_identity, 500).unwrap_err();
        assert!(matches!(err, PackedSegmentError::Invalid { .. }));
        assert!(!segment.has_pending_reservation());

        let valid_identity = PackedObjectIdentity::new(object(1), hash(1), HlcTimestamp::zero());
        let entry = segment.append_without_journal(valid_identity, 500).unwrap();
        assert_eq!(entry.offset, 0);
        assert!(!segment.has_pending_reservation());
    }

    #[test]
    fn a_repacked_segment_names_its_seal_reason() {
        let config = test_config(10_000, 10, 2, 300);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 100);
        segment.seal(200, SealReason::Repacked).unwrap();
        assert_eq!(segment.seal_reason, Some(SealReason::Repacked));
        assert_eq!(SealReason::Repacked.to_string(), "repacked");
    }
}
