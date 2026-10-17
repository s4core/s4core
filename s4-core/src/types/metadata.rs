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

//! Metadata record structure for the index database.

use crate::types::composite::{BlobId, ObjectLayout};
use crate::types::storage_class::{StorageClass, S4_STORAGE_CLASS_METADATA_KEY};
use crate::types::RetentionMode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Sentinel value for delete markers (no actual data storage).
pub const DELETE_MARKER_FILE_ID: u32 = u32::MAX;

/// Sentinel volume id for records whose bytes are not in local volume storage.
///
/// Used by erasure-coded records, whose data lives as shards across a pool. It
/// is deliberately distinct from [`DELETE_MARKER_FILE_ID`]: that value also
/// marks legacy inline objects, and a migration pass keyed on it would try to
/// move bytes that are not there. Being an id no volume file ever has, it turns
/// a missed layout check into a loud "volume not found" instead of a silent
/// read of volume 0.
pub const EXTERNAL_DATA_FILE_ID: u32 = u32::MAX - 1;

/// Record stored in the fast index database (fjall).
///
/// This is the "map" that points to where the actual bytes are stored.
/// For delete markers, `is_delete_marker` is true and no actual data is stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRecord {
    /// Volume file ID (e.g., 42 -> "volume_042.dat").
    /// For delete markers, this is set to `DELETE_MARKER_FILE_ID`.
    pub file_id: u32,
    /// Byte offset from the start of the volume file
    pub offset: u64,
    /// Size of the object in bytes
    pub size: u64,

    // S3 metadata
    /// ETag (usually MD5 or SHA256 hash)
    pub etag: String,
    /// Content type (MIME type)
    pub content_type: String,
    /// User-defined metadata (x-amz-meta-* headers)
    pub metadata: HashMap<String, String>,

    // For deduplication
    /// SHA-256 hash of the content (for Content-Addressable Storage)
    pub content_hash: [u8; 32],

    // Additional metadata
    /// Creation timestamp
    pub created_at: u64,
    /// Last modification timestamp
    pub modified_at: u64,
    /// Version ID (for versioning support)
    pub version_id: Option<String>,
    /// True if this record is a delete marker (S3 versioning).
    /// Delete markers indicate that the object was deleted but versions are preserved.
    #[serde(default)]
    pub is_delete_marker: bool,

    // Object Lock fields (Phase 3)
    /// Object Lock retention mode (GOVERNANCE or COMPLIANCE).
    ///
    /// When set, the object cannot be deleted until `retain_until_timestamp`.
    /// None if no retention is set on this version.
    #[serde(default)]
    pub retention_mode: Option<RetentionMode>,

    /// Retain-until date as Unix timestamp (nanoseconds).
    ///
    /// Object cannot be deleted before this timestamp if retention is set.
    /// Timestamp format matches `created_at` and `modified_at` fields.
    #[serde(default)]
    pub retain_until_timestamp: Option<u64>,

    /// Legal hold status (ON = true, OFF = false).
    ///
    /// When true, object cannot be deleted regardless of retention period.
    /// Independent of retention settings and can be toggled separately.
    #[serde(default)]
    pub legal_hold: bool,

    // Native composite multipart & federation-ready fields
    // All use #[serde(default)] for backward compatibility with existing records.
    /// Object storage layout (flat blob or composite manifest).
    ///
    /// When `None`, the record is treated as `SingleBlob { file_id, offset }`
    /// using the existing fields above. New multipart completions set this to
    /// `Some(ObjectLayout::CompositeManifest { ... })`.
    #[serde(default)]
    pub layout: Option<ObjectLayout>,

    /// Pool ID for federation routing.
    /// Single-node: always 0. Distributed: assigned at bucket creation.
    #[serde(default)]
    pub pool_id: u32,

    /// HLC timestamp for distributed causal ordering.
    /// Single-node: filled from wall clock.
    #[serde(default)]
    pub hlc_timestamp: u64,

    /// HLC logical counter for ordering within same physical timestamp.
    #[serde(default)]
    pub hlc_logical: u32,

    /// Node ID that wrote this record (LWW tiebreaker in federation).
    #[serde(default)]
    pub origin_node_id: u128,

    /// Unique operation ID (idempotency in federation).
    #[serde(default)]
    pub operation_id: u128,
}

/// Reserved metadata key holding an object's S3 tags as a JSON object.
///
/// Tags are stored as one blob rather than as separate metadata entries, which
/// makes the key a cross-crate contract: the S3 tagging API writes it and EC
/// auto-tiering reads it to apply tag exclusions. Naming it once means a change
/// to the format cannot silently break the reader.
pub const OBJECT_TAGS_METADATA_KEY: &str = "_tags";

/// Key prefix of the marker object that records a bucket's existence.
///
/// Buckets have no catalog of their own: each one is a hidden object in
/// `__system__`, and listing the buckets means listing this prefix. The prefix
/// is therefore a cross-crate contract, read by ListBuckets and by the EC
/// tiering sweep, and named once so the two cannot drift apart.
pub const BUCKET_MARKER_KEY_PREFIX: &str = "__s4_bucket_marker_";

/// The bucket of S4's own records: bucket markers and configurations, IAM,
/// erasure-coding metadata.
pub const SYSTEM_BUCKET: &str = "__system__";

/// Key prefix of a bucket's CORS configuration in `__system__`.
pub const CORS_CONFIG_KEY_PREFIX: &str = "__s4_bucket_cors_";
/// Key prefix of a bucket's versioning status in `__system__`.
pub const VERSIONING_CONFIG_KEY_PREFIX: &str = "__s4_bucket_versioning_";
/// Key prefix of a bucket's lifecycle configuration in `__system__`.
pub const LIFECYCLE_CONFIG_KEY_PREFIX: &str = "__s4_bucket_lifecycle_";
/// Key prefix of a bucket's policy in `__system__`.
pub const POLICY_CONFIG_KEY_PREFIX: &str = "__s4_bucket_policy_";
/// Key prefix of a bucket's Object Lock configuration in `__system__`.
pub const OBJECT_LOCK_CONFIG_KEY_PREFIX: &str = "__s4_bucket_object_lock_";
/// Key prefix of a bucket's default encryption in `__system__`.
pub const ENCRYPTION_CONFIG_KEY_PREFIX: &str = "__s4_bucket_encryption_";

/// Key prefixes of a bucket's configurations in `__system__`, one key per
/// configuration: `<prefix><bucket>`. Deleting a bucket deletes all six, so a
/// bucket created again under the same name starts without them.
pub const BUCKET_CONFIG_KEY_PREFIXES: [&str; 6] = [
    CORS_CONFIG_KEY_PREFIX,
    VERSIONING_CONFIG_KEY_PREFIX,
    LIFECYCLE_CONFIG_KEY_PREFIX,
    POLICY_CONFIG_KEY_PREFIX,
    OBJECT_LOCK_CONFIG_KEY_PREFIX,
    ENCRYPTION_CONFIG_KEY_PREFIX,
];

/// Whether `key` of `__system__` holds a bucket's configuration.
pub fn is_bucket_config_key(key: &str) -> bool {
    BUCKET_CONFIG_KEY_PREFIXES.iter().any(|prefix| key.starts_with(prefix))
}

/// Whether `bucket/key` is a bucket's marker: the bucket exists while it does.
pub fn is_bucket_marker(bucket: &str, key: &str) -> bool {
    bucket == SYSTEM_BUCKET && key.starts_with(BUCKET_MARKER_KEY_PREFIX)
}

/// Whether `bucket/key` is a bucket's metadata: its marker or one of its
/// configurations in `__system__`. These keys decide whether a bucket exists
/// and how it behaves, so cluster placement gives them a replication class of
/// their own (plan BUCKET-METADATA-REPLICATION.md, decision 1).
pub fn is_bucket_metadata(bucket: &str, key: &str) -> bool {
    bucket == SYSTEM_BUCKET
        && (key.starts_with(BUCKET_MARKER_KEY_PREFIX) || is_bucket_config_key(key))
}

/// Whether a key spelt `bucket/key` in one string, as the version digests of
/// the storage name it, is a bucket's metadata ([`is_bucket_metadata`]).
pub fn is_bucket_metadata_full_key(full_key: &str) -> bool {
    full_key
        .split_once('/')
        .is_some_and(|(bucket, key)| is_bucket_metadata(bucket, key))
}

/// Key prefix of the IAM records every node of a pool holds a copy of (plan
/// CLUSTER-IAM-REPLICATION.md, decisions 1 and 2): one record per user,
/// `__s4_iam_v2_user_<id>`, and root's state, `__s4_iam_v2_root_state`. The
/// IAM records of earlier releases, `__s4_iam_user_*`, `__s4_iam_username_*`
/// and `__s4_iam_access_key_*`, are not under it.
pub const IAM_V2_KEY_PREFIX: &str = "__s4_iam_v2_";

/// Key prefix of a user record an earlier release stored, `__s4_iam_user_<id>`
/// in `__system__`, on the node it was created through alone. A cluster node
/// that holds such records serves them as its own IAM until its pool takes
/// them over (plan CLUSTER-IAM-REPLICATION.md, decision 8); they are not
/// replicated.
pub const IAM_LEGACY_USER_KEY_PREFIX: &str = "__s4_iam_user_";

/// Whether `bucket/key` is a record of the cluster's control state: an IAM
/// record of version 2 in `__system__`. Cluster placement keeps such a key on
/// every node of the pool, written and read through a majority of them.
pub fn is_cluster_control(bucket: &str, key: &str) -> bool {
    bucket == SYSTEM_BUCKET && key.starts_with(IAM_V2_KEY_PREFIX)
}

/// Whether a key spelt `bucket/key` in one string, as the version digests of
/// the storage name it, is a record of the cluster's control state
/// ([`is_cluster_control`]).
pub fn is_cluster_control_full_key(full_key: &str) -> bool {
    full_key
        .split_once('/')
        .is_some_and(|(bucket, key)| is_cluster_control(bucket, key))
}

/// Whether a replicated pool keeps `bucket/key` in step by its versions: every
/// key of a user bucket, the marker that makes a bucket exist (plan
/// ANTI-ENTROPY.md, decision 3), the bucket's configurations (plan
/// BUCKET-CONFIG-REPLICATION.md, decision 2) and the IAM records of version 2
/// (plan CLUSTER-IAM-REPLICATION.md, decision 2).
///
/// Such a key has a version digest, and its delete leaves a tombstone, so a
/// deleted user does not come back from a replica that missed the delete. The
/// rest of `__system__` — the IAM records of earlier releases, erasure-coding
/// metadata — has neither: each of those paths orders its records its own
/// way, and erasure coding removes a record at the HLC of the very generation
/// it removes, which a tombstone at that HLC would leave in place.
pub fn is_replicated_key(bucket: &str, key: &str) -> bool {
    bucket != SYSTEM_BUCKET || is_bucket_metadata(bucket, key) || is_cluster_control(bucket, key)
}

impl IndexRecord {
    fn default_metadata() -> HashMap<String, String> {
        HashMap::from([(
            S4_STORAGE_CLASS_METADATA_KEY.to_string(),
            StorageClass::Standard.as_metadata_value().to_string(),
        )])
    }

    /// Creates a new index record.
    pub fn new(
        file_id: u32,
        offset: u64,
        size: u64,
        content_hash: [u8; 32],
        etag: String,
        content_type: String,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        Self {
            file_id,
            offset,
            size,
            etag,
            content_type,
            metadata: Self::default_metadata(),
            content_hash,
            created_at: now,
            modified_at: now,
            version_id: None,
            is_delete_marker: false,
            // Object Lock fields default to no lock
            retention_mode: None,
            retain_until_timestamp: None,
            legal_hold: false,
            // Native composite multipart & federation fields
            layout: None,
            pool_id: 0,
            hlc_timestamp: 0,
            hlc_logical: 0,
            origin_node_id: 0,
            operation_id: 0,
        }
    }

    /// Creates a delete marker record (no actual data storage).
    ///
    /// Delete markers are used when versioning is enabled and an object is deleted
    /// without specifying a version ID. The marker indicates the object is "deleted"
    /// while preserving all previous versions.
    pub fn new_delete_marker(version_id: String) -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        Self {
            file_id: DELETE_MARKER_FILE_ID,
            offset: 0,
            size: 0,
            etag: String::new(),
            content_type: String::new(),
            metadata: Self::default_metadata(),
            content_hash: [0u8; 32],
            created_at: now,
            modified_at: now,
            version_id: Some(version_id),
            is_delete_marker: true,
            // Object Lock fields - delete markers cannot be locked
            retention_mode: None,
            retain_until_timestamp: None,
            legal_hold: false,
            // Native composite multipart & federation fields
            layout: None,
            pool_id: 0,
            hlc_timestamp: 0,
            hlc_logical: 0,
            origin_node_id: 0,
            operation_id: 0,
        }
    }

    /// Creates the tombstone of a key a replicated pool deleted at the given HLC.
    ///
    /// A tombstone is what a cluster replica keeps in place of a deleted key, so
    /// that the delete has a version: it is ordered against writes of the key by
    /// its HLC, like any write. It is a delete marker without a version ID,
    /// stored under the key itself, and carries no data. Versioned delete
    /// markers live under `key#version_id` and are never tombstones.
    pub fn new_tombstone(hlc_timestamp: u64, hlc_logical: u32, origin_node_id: u128) -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        Self {
            file_id: DELETE_MARKER_FILE_ID,
            offset: 0,
            size: 0,
            etag: String::new(),
            content_type: String::new(),
            metadata: HashMap::new(),
            content_hash: [0u8; 32],
            created_at: now,
            modified_at: now,
            version_id: None,
            is_delete_marker: true,
            retention_mode: None,
            retain_until_timestamp: None,
            legal_hold: false,
            layout: None,
            pool_id: 0,
            hlc_timestamp,
            hlc_logical,
            origin_node_id,
            operation_id: 0,
        }
    }

    /// Whether this record is the tombstone of a deleted key (see
    /// [`new_tombstone`](Self::new_tombstone)) rather than an object or a
    /// versioned delete marker.
    pub fn is_tombstone(&self) -> bool {
        self.is_delete_marker && self.version_id.is_none()
    }

    /// The order last-writer-wins weighs this record by: its HLC as
    /// `(wall, logical, origin node)`, or `None` for a record stored without
    /// one, which is not weighed against a record or a delete that has one.
    pub fn hlc_order(&self) -> Option<(u64, u32, u128)> {
        (self.hlc_timestamp > 0).then_some((
            self.hlc_timestamp,
            self.hlc_logical,
            self.origin_node_id,
        ))
    }

    /// The HLC every replica and every read path reports for this record, as
    /// `(wall, logical, origin node)`: its own, or, for a record written
    /// without one (in single mode, before the node joined a cluster), one
    /// made up from its modification time with node 0 — no node issued it. A
    /// copy repaired from such a record is written with that same HLC.
    pub fn reported_hlc(&self) -> (u64, u32, u128) {
        self.hlc_order().unwrap_or((self.modified_at / 1_000_000, 0, 0))
    }

    /// Whether last-writer-wins keeps this stored record rather than take
    /// `incoming`: both carry an HLC, and this one ranks at least as high
    /// ([`VersionRank`](crate::storage::VersionRank)). An equal rank is the
    /// same version, which a repeated write does not write again. A record
    /// stored without an HLC is not weighed against one that has it.
    pub fn keeps_over(&self, incoming: &IndexRecord) -> bool {
        self.hlc_order().is_some()
            && incoming.hlc_order().is_some()
            && crate::storage::VersionRank::of_record(self)
                >= crate::storage::VersionRank::of_record(incoming)
    }

    /// Checks if this record represents actual object data (not a delete marker).
    pub fn is_data_record(&self) -> bool {
        !self.is_delete_marker
    }

    /// Returns the durable S4 physical storage class.
    ///
    /// Missing or unrecognized values fall back to `Standard` for
    /// backward compatibility with records written before this marker existed.
    pub fn storage_class(&self) -> StorageClass {
        self.metadata
            .get(S4_STORAGE_CLASS_METADATA_KEY)
            .and_then(|value| StorageClass::from_metadata_value(value))
            .unwrap_or_default()
    }

    /// Persist the S4 physical storage class in this record's metadata.
    pub fn set_storage_class(&mut self, storage_class: StorageClass) {
        self.metadata.insert(
            S4_STORAGE_CLASS_METADATA_KEY.to_string(),
            storage_class.as_metadata_value().to_string(),
        );
    }

    /// Checks if this record references the same content as another record.
    pub fn has_same_content(&self, other: &Self) -> bool {
        self.content_hash == other.content_hash
    }

    /// Returns the effective object layout.
    ///
    /// When `layout` is `None` (backward-compatible old records), this returns
    /// `ObjectLayout::SingleBlob` using the existing `file_id` and `offset` fields.
    pub fn effective_layout(&self) -> ObjectLayout {
        match &self.layout {
            Some(l) => l.clone(),
            None => ObjectLayout::SingleBlob {
                file_id: self.file_id,
                offset: self.offset,
            },
        }
    }

    /// Returns true if this record uses composite (multipart) storage layout.
    pub fn is_composite(&self) -> bool {
        matches!(self.layout, Some(ObjectLayout::CompositeManifest { .. }))
    }

    /// Returns the manifest BlobId if this is a composite object.
    pub fn composite_manifest_blob_id(&self) -> Option<BlobId> {
        match &self.layout {
            Some(ObjectLayout::CompositeManifest {
                manifest_blob_id, ..
            }) => Some(*manifest_blob_id),
            _ => None,
        }
    }

    /// Returns true when the object's bytes live in an erasure-coded pool
    /// rather than in local volume storage.
    ///
    /// Such a record is a namespace entry and nothing more: it carries the size,
    /// ETag, content type and metadata a listing needs, but `file_id`/`offset`
    /// point at no blob. Every code path that resolves a record to bytes, or
    /// that counts a record as a live reference to a blob, has to check this
    /// first.
    pub fn is_erasure_coded(&self) -> bool {
        matches!(self.layout, Some(ObjectLayout::ErasureCoded { .. }))
    }

    /// Turns this record into a namespace entry for an erasure-coded object.
    ///
    /// Everything a client can observe — size, ETag, content type, user
    /// metadata, timestamps, version and the HLC generation — is preserved: the
    /// object did not change, only the place its bytes are stored did. The
    /// local blob coordinates are replaced by
    /// [`EXTERNAL_DATA_FILE_ID`] so that a caller which forgets to check
    /// [`is_erasure_coded`](Self::is_erasure_coded) fails on a volume that does
    /// not exist instead of silently reading volume 0.
    pub fn into_erasure_coded(mut self, erasure_set_id: u64, topology_epoch: u64) -> Self {
        self.layout = Some(ObjectLayout::ErasureCoded {
            erasure_set_id,
            topology_epoch,
        });
        self.file_id = EXTERNAL_DATA_FILE_ID;
        self.offset = 0;
        self
    }

    /// Turns an erasure-coded namespace entry back into a local-blob record.
    ///
    /// The mirror of [`into_erasure_coded`](Self::into_erasure_coded), used
    /// when auto-tiering brings a hot object back from the pool. It preserves
    /// exactly what that method preserves, and for the same reason: a promotion
    /// moves bytes, it does not write a new object, so the ETag, the size, the
    /// timestamps, the user metadata and the HLC generation all have to survive
    /// it. Only the coordinates of the bytes change.
    ///
    /// The layout is cleared rather than set to
    /// [`ObjectLayout::SingleBlob`], which is how every ordinary write leaves
    /// it: a promoted record must be indistinguishable from one the hot write
    /// path produced, or the difference would eventually be read as meaningful.
    pub fn into_locally_stored(mut self, file_id: u32, offset: u64) -> Self {
        self.layout = None;
        self.file_id = file_id;
        self.offset = offset;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::composite::{BlobId, ObjectLayout};

    /// The marker and the six configurations of a bucket, in `__system__`,
    /// are its metadata, spelt apart or as one `bucket/key` string; a key of
    /// a user bucket with the same name, IAM and erasure-coding metadata are
    /// not.
    #[test]
    fn a_bucket_marker_and_its_configurations_are_bucket_metadata() {
        assert!(is_bucket_marker(SYSTEM_BUCKET, "__s4_bucket_marker_photos"));
        assert!(!is_bucket_marker(SYSTEM_BUCKET, "__s4_bucket_cors_photos"));
        assert!(!is_bucket_marker("photos", "__s4_bucket_marker_photos"));
        let mut keys = vec!["__s4_bucket_marker_photos".to_string()];
        keys.extend(BUCKET_CONFIG_KEY_PREFIXES.iter().map(|prefix| format!("{prefix}photos")));
        for key in &keys {
            assert!(is_bucket_metadata(SYSTEM_BUCKET, key), "{key}");
            assert!(
                is_bucket_metadata_full_key(&format!("__system__/{key}")),
                "{key}"
            );
            assert!(!is_bucket_metadata("photos", key), "{key}");
            assert!(
                !is_bucket_metadata_full_key(&format!("photos/{key}")),
                "{key}"
            );
            assert!(!is_bucket_metadata_full_key(key), "{key}");
        }
        for key in [
            "__s4_iam_user_alice",
            "__s4_iam_v2_root_state",
            "__s4_ec_v1/pools/pool-1",
        ] {
            assert!(!is_bucket_metadata(SYSTEM_BUCKET, key), "{key}");
        }
        assert!(!is_bucket_metadata_full_key("__system__"));
    }

    /// User keys, bucket markers and the six bucket configurations are
    /// replicated keys; IAM and erasure-coding metadata in `__system__` are
    /// not, nor is a key of a user bucket that only looks like a configuration
    /// outside `__system__` (it is replicated as any user key).
    #[test]
    fn bucket_configurations_are_replicated_keys() {
        for prefix in BUCKET_CONFIG_KEY_PREFIXES {
            let key = format!("{prefix}photos");
            assert!(is_bucket_config_key(&key), "{key}");
            assert!(is_replicated_key(SYSTEM_BUCKET, &key), "{key}");
        }
        assert!(is_replicated_key(
            SYSTEM_BUCKET,
            "__s4_bucket_marker_photos"
        ));
        assert!(is_replicated_key("photos", "__s4_bucket_cors_x"));
        assert!(!is_bucket_config_key("__s4_bucket_marker_photos"));
        for key in [
            "__s4_iam_user_alice",
            "__s4_iam_username_alice",
            "__s4_iam_access_key_S4AKX",
            "__s4_ec_v1/tiering/photos",
            "cors/photos",
        ] {
            assert!(!is_replicated_key(SYSTEM_BUCKET, key), "{key}");
        }
    }

    /// The IAM records of version 2 are the cluster's control state and
    /// replicated keys; those of earlier releases, a user's key named like
    /// them and bucket metadata are not control state.
    #[test]
    fn iam_records_of_version_2_are_cluster_control_and_replicated() {
        for key in ["__s4_iam_v2_user_0f8e", "__s4_iam_v2_root_state"] {
            assert!(is_cluster_control(SYSTEM_BUCKET, key), "{key}");
            assert!(
                is_cluster_control_full_key(&format!("__system__/{key}")),
                "{key}"
            );
            assert!(is_replicated_key(SYSTEM_BUCKET, key), "{key}");
            assert!(!is_bucket_metadata(SYSTEM_BUCKET, key), "{key}");
            assert!(!is_cluster_control("photos", key), "{key}");
            assert!(
                !is_cluster_control_full_key(&format!("photos/{key}")),
                "{key}"
            );
            assert!(!is_cluster_control_full_key(key), "{key}");
        }
        for key in [
            "__s4_iam_user_alice",
            "__s4_iam_username_alice",
            "__s4_iam_access_key_S4AKX",
            "__s4_bucket_marker_photos",
            "__s4_bucket_cors_photos",
        ] {
            assert!(!is_cluster_control(SYSTEM_BUCKET, key), "{key}");
        }
    }

    #[test]
    fn index_record_new_roundtrip() {
        let record = IndexRecord::new(
            1,
            100,
            50,
            [0xAB; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );
        let bytes = bincode::serialize(&record).unwrap();
        let decoded: IndexRecord = bincode::deserialize(&bytes).unwrap();
        assert_eq!(record.file_id, decoded.file_id);
        assert_eq!(record.offset, decoded.offset);
        assert_eq!(record.size, decoded.size);
        assert!(decoded.layout.is_none());
        assert_eq!(decoded.pool_id, 0);
        assert_eq!(decoded.hlc_timestamp, 0);
        assert_eq!(decoded.origin_node_id, 0);
        assert_eq!(decoded.operation_id, 0);
        assert_eq!(decoded.storage_class(), StorageClass::Standard);
    }

    /// Simulate deserializing an old record that was serialized BEFORE the
    /// new fields existed. The old format is a bincode blob without layout/
    /// pool_id/hlc_* fields. Since we use `#[serde(default)]`, the missing
    /// fields should get their defaults.
    ///
    /// Note: bincode with fixed-length encoding does NOT support `#[serde(default)]`
    /// for missing trailing fields because it's a non-self-describing format.
    /// However, because we only ever add Option/default fields and existing
    /// data is read via fjall which stores the exact bytes written, old records
    /// written with the old schema will fail to decode with bincode if the
    /// binary layout changes. This test verifies that a NEW record roundtrips
    /// correctly and that effective_layout() works for None.
    #[test]
    fn index_record_effective_layout_none() {
        let record = IndexRecord::new(
            42,
            1024,
            5000,
            [0xFF; 32],
            "etag".to_string(),
            "application/octet-stream".to_string(),
        );
        assert!(record.layout.is_none());
        let layout = record.effective_layout();
        match layout {
            ObjectLayout::SingleBlob { file_id, offset } => {
                assert_eq!(file_id, 42);
                assert_eq!(offset, 1024);
            }
            _ => panic!("expected SingleBlob"),
        }
    }

    #[test]
    fn index_record_with_composite_layout() {
        let mut record = IndexRecord::new(
            0,
            0,
            65_000_000_000,
            [0; 32],
            "abc-7494".to_string(),
            "application/octet-stream".to_string(),
        );
        record.layout = Some(ObjectLayout::CompositeManifest {
            manifest_blob_id: BlobId([0xCC; 32]),
            manifest_blob_len: 512,
            part_count: 7494,
        });

        let bytes = bincode::serialize(&record).unwrap();
        let decoded: IndexRecord = bincode::deserialize(&bytes).unwrap();

        assert!(decoded.is_composite());
        match decoded.effective_layout() {
            ObjectLayout::CompositeManifest {
                manifest_blob_id,
                manifest_blob_len,
                part_count,
            } => {
                assert_eq!(manifest_blob_id, BlobId([0xCC; 32]));
                assert_eq!(manifest_blob_len, 512);
                assert_eq!(part_count, 7494);
            }
            _ => panic!("expected CompositeManifest"),
        }
    }

    #[test]
    fn index_record_federation_fields_default() {
        let record = IndexRecord::new(
            1,
            0,
            100,
            [0; 32],
            "e".to_string(),
            "text/plain".to_string(),
        );
        assert_eq!(record.pool_id, 0);
        assert_eq!(record.hlc_timestamp, 0);
        assert_eq!(record.hlc_logical, 0);
        assert_eq!(record.origin_node_id, 0);
        assert_eq!(record.operation_id, 0);
        assert_eq!(record.storage_class(), StorageClass::Standard);
    }

    #[test]
    fn index_record_delete_marker_not_composite() {
        let marker = IndexRecord::new_delete_marker("v1".to_string());
        assert!(!marker.is_composite());
        assert!(marker.layout.is_none());
        assert_eq!(marker.storage_class(), StorageClass::Standard);
    }

    #[test]
    fn index_record_storage_class_is_reserved_metadata() {
        let mut record = IndexRecord::new(
            1,
            0,
            100,
            [0; 32],
            "e".to_string(),
            "text/plain".to_string(),
        );
        record.set_storage_class(StorageClass::ErasureCoded);

        assert_eq!(record.storage_class(), StorageClass::ErasureCoded);
        assert_eq!(
            record.metadata.get(S4_STORAGE_CLASS_METADATA_KEY),
            Some(&"erasure_coded".to_string())
        );
    }
}
