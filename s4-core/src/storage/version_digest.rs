// Copyright 2026 S4Core Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Version digests: a compact description of the version each replicated key
//! holds, kept by the index itself.
//!
//! In cluster mode the index keeps one digest per replicated key in the
//! `version_digests` keyspace, changed in the same atomic batch as the record
//! it describes ([`IndexDb`](super::IndexDb)). Anti-entropy compares replicas
//! by these digests instead of reading every record.
//!
//! # What has a digest
//!
//! The keys a cluster replicates through its coordinator: the keys of user
//! buckets, the bucket markers (`__system__/__s4_bucket_marker_*`) and the
//! bucket configurations (`__system__/__s4_bucket_{cors,…}_*`). Not the rest
//! of `__system__` (IAM, erasure-coding metadata),
//! not versioned records (`key#version_id`, versioning is not used in cluster
//! mode), not multipart state, which lives in keyspaces of its own.
//!
//! # Layout
//!
//! | Part  | Bytes | Content |
//! |-------|-------|---------|
//! | key   | 8     | [`ring_position`](super::placement::ring_position) of the key, big-endian |
//! | key   | rest  | the key as the index holds it, `{bucket}/{key}` |
//! | value | 61    | HLC wall time, HLC logical, origin node, ETag hash, kind |
//!
//! The ring position first makes the keys of one ring range a contiguous scan.

use super::placement::ring_position_of_bytes;
use crate::error::StorageError;
use crate::types::{is_replicated_key, IndexRecord};

/// Version of the digest layout and of the set of keys that have a digest. A
/// stored layout of another version, or none, makes the index rebuild every
/// digest from its records when it opens. Version 4 gave the bucket
/// configurations digests, version 5 the IAM records of version 2.
pub const VERSION_DIGESTS_FORMAT: u32 = 5;

/// The bucket markers' prefix in the form callers write them, before the
/// index moves them to the `buckets` keyspace.
pub(crate) const BUCKET_MARKER_PREFIX: &str = "__system__/__s4_bucket_marker_";

/// The prefix of the IAM keys in the form callers write them, before the index
/// moves them to the `iam` keyspace.
pub(crate) const IAM_PREFIX: &str = "__system__/__s4_iam_";

/// The prefix of the keys of the system bucket in the `objects` keyspace.
const SYSTEM_BUCKET_PREFIX: &[u8] = b"__system__/";

const VALUE_LEN: usize = 8 + 4 + 16 + 32 + 1;

/// What a replica holds for a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VersionKind {
    /// An object with its data on this replica.
    Live,
    /// The key's tombstone: the delete, kept with its HLC.
    Tombstone,
    /// A namespace entry for an object whose data moved to erasure coding.
    ErasureCoded,
}

impl VersionKind {
    /// What `record` is.
    pub fn of_record(record: &IndexRecord) -> Self {
        if record.is_tombstone() {
            Self::Tombstone
        } else if record.is_erasure_coded() {
            Self::ErasureCoded
        } else {
            Self::Live
        }
    }

    /// Its place among the versions of one HLC: an object gives way to the
    /// erasure-coded entry it was moved to, and both to the delete.
    fn precedence(self) -> u8 {
        match self {
            Self::Live => 0,
            Self::ErasureCoded => 1,
            Self::Tombstone => 2,
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Live => 1,
            Self::Tombstone => 2,
            Self::ErasureCoded => 3,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Live),
            2 => Some(Self::Tombstone),
            3 => Some(Self::ErasureCoded),
            _ => None,
        }
    }
}

/// Where an ETag ranks among the versions of one HLC and kind.
///
/// An ETag of the multipart form (`<hex>-<parts>`) ranks above any other: only
/// the completion of an upload gives one, never the bytes, so a copy that was
/// stored without its source's ETag carries the MD5 of its bytes instead, and
/// the multipart one is the ETag the client was given. Past that, the greater
/// BLAKE3 of the ETag: an arbitrary order, but the same on every node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EtagRank {
    multipart: bool,
    hash: [u8; 32],
}

impl EtagRank {
    /// The rank of a version without an ETag: a tombstone.
    pub const NONE: Self = Self {
        multipart: false,
        hash: [0; 32],
    };

    /// The rank of `etag`.
    pub fn of(etag: &str) -> Self {
        Self {
            multipart: is_multipart_etag(etag),
            hash: *blake3::hash(etag.as_bytes()).as_bytes(),
        }
    }

    /// The BLAKE3 of the ETag; zero for [`Self::NONE`].
    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }
}

/// Whether `etag` has the form a multipart upload gives: hex digits, a dash
/// and the number of parts, quoted or not.
fn is_multipart_etag(etag: &str) -> bool {
    etag.trim_matches('"').split_once('-').is_some_and(|(hash, parts)| {
        !hash.is_empty()
            && hash.bytes().all(|b| b.is_ascii_hexdigit())
            && !parts.is_empty()
            && parts.bytes().all(|b| b.is_ascii_digit())
    })
}

/// The rank of a version of a key: the one total order every node weighs
/// versions by.
///
/// Ordered by HLC first (`(wall, logical, origin node)`, made up for a record
/// written without one: [`IndexRecord::reported_hlc`]); at one HLC an object
/// gives way to its erasure-coded entry and both to a tombstone; between
/// objects of one HLC the [`EtagRank`] decides. Last-writer-wins in storage,
/// the quorum read, the listing and anti-entropy all weigh versions by it, and
/// two replicas hold the same version exactly when their ranks are equal — the
/// [`VersionDigest`] describes the same three parts. Because the order is
/// total and the same everywhere, replicas reach one version from any state
/// and in any order of deliveries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionRank {
    /// The HLC, as `(wall, logical, origin node)`.
    pub hlc: (u64, u32, u128),
    /// What the version is.
    pub kind: VersionKind,
    /// The ETag's rank; [`EtagRank::NONE`] for a tombstone.
    pub etag: EtagRank,
}

impl VersionRank {
    /// The rank of a version of `kind` at `hlc` with ETag `etag`, which a
    /// tombstone does not have.
    pub fn new(hlc: (u64, u32, u128), kind: VersionKind, etag: &str) -> Self {
        Self {
            hlc,
            kind,
            etag: match kind {
                VersionKind::Tombstone => EtagRank::NONE,
                VersionKind::Live | VersionKind::ErasureCoded => EtagRank::of(etag),
            },
        }
    }

    /// The rank of `record`.
    pub fn of_record(record: &IndexRecord) -> Self {
        Self::new(
            record.reported_hlc(),
            VersionKind::of_record(record),
            &record.etag,
        )
    }
}

impl Ord for VersionRank {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hlc
            .cmp(&other.hlc)
            .then_with(|| self.kind.precedence().cmp(&other.kind.precedence()))
            .then_with(|| self.etag.cmp(&other.etag))
    }
}

impl PartialOrd for VersionRank {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// The digest of the version a replica holds for a key.
///
/// Two replicas hold the same version of a key exactly when their digests are
/// equal, as when their [`VersionRank`]s are. Where the data sits on disk is
/// not part of it: compaction moves data without changing the version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionDigest {
    /// HLC wall time of the write or delete; for a record written without
    /// an HLC, the one made up for it ([`IndexRecord::reported_hlc`]), which a
    /// copy repaired from it stores.
    pub hlc_timestamp: u64,
    /// HLC logical counter.
    pub hlc_logical: u32,
    /// Node that issued the HLC.
    pub origin_node_id: u128,
    /// BLAKE3 of the ETag; zero for a tombstone.
    ///
    /// The ETag, and not the record's content hash, because the content hash
    /// describes how a replica stores the bytes: an object completed by a
    /// multipart upload stores none (it is a list of parts), while a copy of
    /// it repaired onto another replica is one blob with a hash of its own. A
    /// repaired copy keeps the ETag of its version
    /// ([`REPAIRED_ETAG_METADATA_KEY`](crate::storage::engine::REPAIRED_ETAG_METADATA_KEY)).
    pub etag_hash: [u8; 32],
    /// What the replica holds.
    pub kind: VersionKind,
}

impl VersionDigest {
    /// The digest of a record.
    pub fn of_record(record: &IndexRecord) -> Self {
        Self::of_rank(&VersionRank::of_record(record))
    }

    /// The digest of the version ranked `rank`.
    pub fn of_rank(rank: &VersionRank) -> Self {
        let (hlc_timestamp, hlc_logical, origin_node_id) = rank.hlc;
        Self {
            hlc_timestamp,
            hlc_logical,
            origin_node_id,
            etag_hash: rank.etag.hash(),
            kind: rank.kind,
        }
    }

    /// The stored form of the digest.
    pub fn encode(&self) -> [u8; VALUE_LEN] {
        let mut value = [0u8; VALUE_LEN];
        value[..8].copy_from_slice(&self.hlc_timestamp.to_be_bytes());
        value[8..12].copy_from_slice(&self.hlc_logical.to_be_bytes());
        value[12..28].copy_from_slice(&self.origin_node_id.to_be_bytes());
        value[28..60].copy_from_slice(&self.etag_hash);
        value[60] = self.kind.code();
        value
    }

    /// Reads a digest back from its stored form.
    pub fn decode(value: &[u8]) -> Result<Self, StorageError> {
        let malformed = || {
            StorageError::Serialization(format!(
                "a version digest of {} byte(s), expected {VALUE_LEN}",
                value.len()
            ))
        };
        if value.len() != VALUE_LEN {
            return Err(malformed());
        }
        let mut wall = [0u8; 8];
        wall.copy_from_slice(&value[..8]);
        let mut logical = [0u8; 4];
        logical.copy_from_slice(&value[8..12]);
        let mut node = [0u8; 16];
        node.copy_from_slice(&value[12..28]);
        let mut etag_hash = [0u8; 32];
        etag_hash.copy_from_slice(&value[28..60]);
        let kind = VersionKind::from_code(value[60]).ok_or_else(|| {
            StorageError::Serialization(format!("a version digest of kind {}", value[60]))
        })?;
        Ok(Self {
            hlc_timestamp: u64::from_be_bytes(wall),
            hlc_logical: u32::from_be_bytes(logical),
            origin_node_id: u128::from_be_bytes(node),
            etag_hash,
            kind,
        })
    }

    /// Whether a replica holding this version is not behind one holding
    /// `other`: a later HLC, or at the same HLC a kind that takes precedence
    /// ([`VersionRank`]), or the very same version. At one HLC and kind two
    /// different ETags cannot be ordered by their digests alone, and neither
    /// counts as covering the other.
    pub fn covers(&self, other: &VersionDigest) -> bool {
        let hlc = |digest: &VersionDigest| {
            (
                digest.hlc_timestamp,
                digest.hlc_logical,
                digest.origin_node_id,
            )
        };
        match hlc(self).cmp(&hlc(other)) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => match self.kind.precedence().cmp(&other.kind.precedence())
            {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Equal => self.etag_hash == other.etag_hash,
            },
        }
    }
}

/// The HLC from which every delete on a node left a tombstone.
///
/// A key a replica does not hold, while another holds a copy written before
/// the replica's horizon, may have been deleted there before tombstones
/// existed: that absence proves nothing, and the copy must not be brought back
/// on its strength. Zero for a node that held no replicated record when it
/// started keeping tombstones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TombstoneHorizon {
    /// HLC wall time.
    pub hlc_timestamp: u64,
    /// HLC logical counter.
    pub hlc_logical: u32,
    /// Node that issued the HLC.
    pub origin_node_id: u128,
}

impl TombstoneHorizon {
    /// The horizon of a node that never deleted without a tombstone.
    pub const ZERO: Self = Self {
        hlc_timestamp: 0,
        hlc_logical: 0,
        origin_node_id: 0,
    };

    const LEN: usize = 8 + 4 + 16;

    pub(crate) fn encode(&self) -> [u8; Self::LEN] {
        let mut value = [0u8; Self::LEN];
        value[..8].copy_from_slice(&self.hlc_timestamp.to_be_bytes());
        value[8..12].copy_from_slice(&self.hlc_logical.to_be_bytes());
        value[12..].copy_from_slice(&self.origin_node_id.to_be_bytes());
        value
    }

    pub(crate) fn decode(value: &[u8]) -> Result<Self, StorageError> {
        if value.len() != Self::LEN {
            return Err(StorageError::Serialization(format!(
                "a tombstone horizon of {} byte(s), expected {}",
                value.len(),
                Self::LEN
            )));
        }
        let mut wall = [0u8; 8];
        wall.copy_from_slice(&value[..8]);
        let mut logical = [0u8; 4];
        logical.copy_from_slice(&value[8..12]);
        let mut node = [0u8; 16];
        node.copy_from_slice(&value[12..]);
        Ok(Self {
            hlc_timestamp: u64::from_be_bytes(wall),
            hlc_logical: u32::from_be_bytes(logical),
            origin_node_id: u128::from_be_bytes(node),
        })
    }
}

/// One stored digest with the key it describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionDigestEntry {
    /// Position of the key on the placement ring.
    pub ring_position: u64,
    /// The key as the index holds it: `{bucket}/{key}`, a bucket marker as
    /// `__system__/__s4_bucket_marker_{bucket}`.
    pub key: String,
    /// The version the key holds.
    pub digest: VersionDigest,
}

impl VersionDigestEntry {
    /// Reads an entry back from a stored key and value.
    pub(crate) fn decode(key: &[u8], value: &[u8]) -> Result<Self, StorageError> {
        if key.len() < 8 {
            return Err(StorageError::Serialization(format!(
                "a version digest key of {} byte(s)",
                key.len()
            )));
        }
        let mut position = [0u8; 8];
        position.copy_from_slice(&key[..8]);
        let name = std::str::from_utf8(&key[8..])
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(Self {
            ring_position: u64::from_be_bytes(position),
            key: name.to_string(),
            digest: VersionDigest::decode(value)?,
        })
    }
}

/// Which keyspace of the index a digest key belongs to, as far as digests go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DigestSource {
    /// The `objects` keyspace: keys are `{bucket}/{key}`.
    Objects,
    /// The `buckets` keyspace: keys are bucket names, markers of
    /// `__system__/__s4_bucket_marker_{name}`.
    Buckets,
    /// The `iam` keyspace: keys are what follows `__system__/__s4_iam_`. Only
    /// the records of version 2 among them have digests.
    Iam,
}

/// The change one write to a covered keyspace makes to the digests: the
/// digest key, and the new value or `None` for a removal.
pub(crate) type DigestChange = (Vec<u8>, Option<Vec<u8>>);

/// The digest change of writing `value` (or removing, for `None`) at
/// `stripped_key` of `source`, or `None` when that key never has a digest.
///
/// A record that has no digest (a versioned one) removes whatever digest the
/// key had: the digests always describe what the key holds now.
pub(crate) fn digest_change(
    source: DigestSource,
    stripped_key: &[u8],
    value: Option<&[u8]>,
) -> Result<Option<DigestChange>, StorageError> {
    let digest_key = match source {
        DigestSource::Objects => {
            if !is_replicated_full_key(stripped_key) {
                return Ok(None);
            }
            digest_key(stripped_key)
        }
        DigestSource::Buckets => {
            let mut full = Vec::with_capacity(BUCKET_MARKER_PREFIX.len() + stripped_key.len());
            full.extend_from_slice(BUCKET_MARKER_PREFIX.as_bytes());
            full.extend_from_slice(stripped_key);
            digest_key(&full)
        }
        DigestSource::Iam => {
            let mut full = Vec::with_capacity(IAM_PREFIX.len() + stripped_key.len());
            full.extend_from_slice(IAM_PREFIX.as_bytes());
            full.extend_from_slice(stripped_key);
            if !is_replicated_full_key(&full) {
                return Ok(None);
            }
            digest_key(&full)
        }
    };
    let Some(value) = value else {
        return Ok(Some((digest_key, None)));
    };
    let record: IndexRecord =
        bincode::deserialize(value).map_err(|e| StorageError::Serialization(e.to_string()))?;
    let digest = match source {
        DigestSource::Objects if record.version_id.is_some() => None,
        _ => Some(VersionDigest::of_record(&record).encode().to_vec()),
    };
    Ok(Some((digest_key, digest)))
}

/// Whether a key held as `{bucket}/{key}` is one the pool replicates
/// ([`is_replicated_key`]): the same rule that decides which deletes leave a
/// tombstone decides which keys have a digest.
fn is_replicated_full_key(full_key: &[u8]) -> bool {
    match std::str::from_utf8(full_key) {
        Ok(full_key) => full_key
            .split_once('/')
            .is_none_or(|(bucket, key)| is_replicated_key(bucket, key)),
        Err(_) => !full_key.starts_with(SYSTEM_BUCKET_PREFIX),
    }
}

/// The digest key of a key held as `{bucket}/{key}`.
pub(crate) fn digest_key_of(full_key: &str) -> Vec<u8> {
    digest_key(full_key.as_bytes())
}

/// The digest key of a key held as `{bucket}/{key}`.
fn digest_key(full_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + full_key.len());
    key.extend_from_slice(&ring_position_of_bytes(full_key).to_be_bytes());
    key.extend_from_slice(full_key);
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::placement::ring_position;

    fn record(wall: u64) -> IndexRecord {
        let mut record = IndexRecord::new(
            3,
            128,
            5,
            [7; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );
        record.hlc_timestamp = wall;
        record.hlc_logical = 2;
        record.origin_node_id = 9;
        record
    }

    fn change(source: DigestSource, key: &str, record: &IndexRecord) -> Option<DigestChange> {
        let value = bincode::serialize(record).unwrap();
        digest_change(source, key.as_bytes(), Some(&value)).unwrap()
    }

    /// One total order: HLC first; at one HLC an object gives way to its
    /// erasure-coded entry and both to the tombstone; between objects the
    /// multipart ETag outranks any other, then the ETag's BLAKE3.
    #[test]
    fn versions_rank_by_hlc_then_kind_then_etag() {
        let rank = |wall: u64, kind, etag: &str| VersionRank::new((wall, 0, 7), kind, etag);
        let multipart = "9b2cf535f27731c974343645a3985328-2";
        let md5 = "5d41402abc4b2a76b9719d911017c592";

        assert!(rank(20, VersionKind::Live, md5) > rank(10, VersionKind::Tombstone, ""));
        assert!(rank(20, VersionKind::Tombstone, "") > rank(20, VersionKind::ErasureCoded, md5));
        assert!(rank(20, VersionKind::ErasureCoded, md5) > rank(20, VersionKind::Live, md5));
        assert!(rank(20, VersionKind::Live, multipart) > rank(20, VersionKind::Live, md5));
        assert!(
            rank(20, VersionKind::Live, &format!("\"{multipart}\""))
                > rank(20, VersionKind::Live, md5)
        );
        assert_eq!(
            rank(20, VersionKind::Tombstone, "anything"),
            rank(20, VersionKind::Tombstone, "")
        );
        let (a, b) = (
            rank(20, VersionKind::Live, "aaa"),
            rank(20, VersionKind::Live, "bbb"),
        );
        assert_ne!(a, b);
        assert_eq!(a.cmp(&b), b.cmp(&a).reverse());
        assert!(!is_multipart_etag("abc-"));
        assert!(!is_multipart_etag("-2"));
        assert!(!is_multipart_etag("xyz-2"));
    }

    /// A record written without an HLC and a copy that stores the HLC made up
    /// for it describe one version: their digests are equal.
    #[test]
    fn a_record_without_an_hlc_and_its_copy_have_one_digest() {
        let mut original = record(0);
        original.modified_at = 1_700_000_000_123_456_789;
        let mut copy = original.clone();
        copy.hlc_timestamp = original.modified_at / 1_000_000;
        copy.hlc_logical = 0;
        copy.origin_node_id = 0;
        copy.modified_at += 5_000_000_000;
        assert_eq!(
            VersionDigest::of_record(&original),
            VersionDigest::of_record(&copy)
        );
        assert_eq!(
            VersionRank::of_record(&original),
            VersionRank::of_record(&copy)
        );
    }

    #[test]
    fn a_digest_reads_back_as_written() {
        for record in [record(10), IndexRecord::new_tombstone(20, 1, 3), record(0)] {
            let digest = VersionDigest::of_record(&record);
            assert_eq!(VersionDigest::decode(&digest.encode()).unwrap(), digest);
        }
        assert!(VersionDigest::decode(&[0; 60]).is_err());
        let mut unknown_kind = VersionDigest::of_record(&record(1)).encode();
        unknown_kind[60] = 9;
        assert!(VersionDigest::decode(&unknown_kind).is_err());
    }

    #[test]
    fn a_horizon_reads_back_as_written() {
        let horizon = TombstoneHorizon {
            hlc_timestamp: 1_700_000_000_000,
            hlc_logical: 4,
            origin_node_id: u128::MAX - 1,
        };
        assert_eq!(
            TombstoneHorizon::decode(&horizon.encode()).unwrap(),
            horizon
        );
        assert!(TombstoneHorizon::decode(&[0; 27]).is_err());
        assert!(TombstoneHorizon::ZERO < horizon);
    }

    #[test]
    fn the_digest_names_the_version_and_not_where_its_data_sits() {
        let written = record(10);
        let mut moved = written.clone();
        moved.file_id = 8;
        moved.offset = 4096;
        assert_eq!(
            VersionDigest::of_record(&written),
            VersionDigest::of_record(&moved),
            "compaction moves data without changing the version"
        );

        let mut rewritten = written.clone();
        rewritten.hlc_logical += 1;
        assert_ne!(
            VersionDigest::of_record(&written),
            VersionDigest::of_record(&rewritten)
        );

        let tombstone = VersionDigest::of_record(&IndexRecord::new_tombstone(10, 2, 9));
        assert_eq!(tombstone.kind, VersionKind::Tombstone);
        assert_eq!(tombstone.etag_hash, [0; 32]);
        assert_ne!(tombstone, VersionDigest::of_record(&written));
    }

    /// A version is the same however a replica stores its bytes: an object
    /// completed by a multipart upload holds no content hash, and a copy of it
    /// repaired onto another replica, one blob, holds one — with the same ETag.
    #[test]
    fn a_repaired_copy_of_a_multipart_object_is_the_same_version() {
        let mut completed = record(10);
        completed.content_hash = [0; 32];
        completed.etag = "\"9b2cf535f27731c974343645a3985328-2\"".to_string();
        let mut repaired = completed.clone();
        repaired.content_hash = [7; 32];
        assert_eq!(
            VersionDigest::of_record(&completed),
            VersionDigest::of_record(&repaired)
        );
        let mut other_content = repaired.clone();
        other_content.etag = "\"0cc175b9c0f1b6a831c399e269772661\"".to_string();
        assert_ne!(
            VersionDigest::of_record(&completed),
            VersionDigest::of_record(&other_content)
        );
    }

    #[test]
    fn a_digest_key_is_ordered_by_ring_position() {
        let (key, value) = change(DigestSource::Objects, "photos/cat.jpg", &record(1)).unwrap();
        let entry = VersionDigestEntry::decode(&key, value.as_deref().unwrap()).unwrap();
        assert_eq!(entry.ring_position, ring_position("photos", "cat.jpg"));
        assert_eq!(entry.key, "photos/cat.jpg");

        let (key, _) = change(DigestSource::Buckets, "photos", &record(1)).unwrap();
        let entry =
            VersionDigestEntry::decode(&key, &VersionDigest::of_record(&record(1)).encode())
                .unwrap();
        assert_eq!(
            entry.ring_position,
            ring_position("__system__", "__s4_bucket_marker_photos"),
            "a bucket marker sits where placement puts it"
        );
        assert_eq!(entry.key, "__system__/__s4_bucket_marker_photos");
    }

    #[test]
    fn only_replicated_keys_have_digests() {
        assert!(change(DigestSource::Objects, "__system__/cors/photos", &record(1)).is_none());
        assert!(
            digest_change(DigestSource::Objects, b"__system__/cors/photos", None)
                .unwrap()
                .is_none()
        );

        let mut versioned = record(1);
        versioned.version_id = Some("v1".to_string());
        let (_, value) = change(DigestSource::Objects, "photos/cat.jpg#v1", &versioned).unwrap();
        assert_eq!(
            value, None,
            "a versioned record removes, never writes, a digest"
        );

        let coded = record(1).into_erasure_coded(4, 1);
        let (_, value) = change(DigestSource::Objects, "photos/cat.jpg", &coded).unwrap();
        assert_eq!(
            VersionDigest::decode(&value.unwrap()).unwrap().kind,
            VersionKind::ErasureCoded
        );

        let (_, value) =
            digest_change(DigestSource::Objects, b"photos/cat.jpg", None).unwrap().unwrap();
        assert_eq!(value, None);
    }

    /// A replica covers another when it is not behind it: a later HLC, a
    /// kind of precedence at the same HLC, or the very same version; two
    /// ETags at one HLC and kind are not ordered by their digests.
    #[test]
    fn a_digest_covers_what_is_not_newer_than_it() {
        let at = |wall: u64, kind: VersionKind, etag: u8| VersionDigest {
            hlc_timestamp: wall,
            hlc_logical: 0,
            origin_node_id: 1,
            etag_hash: [etag; 32],
            kind,
        };
        let live = at(10, VersionKind::Live, 1);
        assert!(live.covers(&live));
        assert!(at(11, VersionKind::Live, 2).covers(&live));
        assert!(!live.covers(&at(11, VersionKind::Live, 1)));
        assert!(at(10, VersionKind::Tombstone, 0).covers(&live));
        assert!(!live.covers(&at(10, VersionKind::Tombstone, 0)));
        assert!(at(10, VersionKind::ErasureCoded, 1).covers(&live));
        assert!(!live.covers(&at(10, VersionKind::Live, 2)));
        assert!(!at(10, VersionKind::Live, 2).covers(&live));
    }
}
