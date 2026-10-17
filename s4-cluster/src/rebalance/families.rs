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

//! Which keys the move carries to the new owners, and how it finds them.
//!
//! Everything placed by the pool's ring moves (plan
//! ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, «Что размещается по
//! кольцу пула»):
//!
//! - the keys with version digests — every key of a user bucket, the bucket
//!   markers and configurations, the IAM records of version 2 — are read
//!   from the digests, a range at a time ([`TransferPath::Digests`]);
//! - the replicated records of `__system__` without digests — the tiering
//!   policies, the pool's roster of erasure sets and their layouts, the pool
//!   generation — are listed by their prefixes ([`TransferPath::SystemRecords`]);
//! - the open multipart uploads are read from the node's sessions
//!   ([`TransferPath::MultipartSessions`]).
//!
//! What is placed otherwise stays ([`TransferPath::Stays`]): the shards,
//! manifests and small-object placements of erasure coding live on their
//! sets, the node's own IAM state on the node, and the records of earlier
//! releases are read only where they were written.

use s4_core::types::{is_replicated_key, SYSTEM_BUCKET};

/// The prefixes in `__system__` of the replicated records without version
/// digests, listed by the move.
pub const SYSTEM_RECORD_PREFIXES: [&str; 3] = [
    "__s4_ec_v1/tiering/",
    "__s4_ec_v1/pools/",
    "__s4_pool_generation_v1/",
];

/// Why a family stays where it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StayReason {
    /// Placed on its erasure set by the set's layout, not by the ring.
    OnItsErasureSet,
    /// This node's own state, never replicated.
    NodeLocal,
    /// A record of an earlier release, read on the node that wrote it.
    EarlierRelease,
}

/// How the move finds the keys of a family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferPath {
    /// Read from the version digests, a range at a time.
    Digests,
    /// Listed by the prefixes of [`SYSTEM_RECORD_PREFIXES`].
    SystemRecords,
    /// Read from the node's multipart sessions.
    MultipartSessions,
    /// Not moved.
    Stays(StayReason),
}

/// The families of `__system__`, by prefix, longest first where one prefix
/// begins another. Every key the code names under `__s4_` falls in one of
/// them (`every_system_key_the_code_names_has_a_family`).
const SYSTEM_FAMILIES: [(&str, TransferPath); 20] = [
    ("__s4_bucket_marker_", TransferPath::Digests),
    ("__s4_bucket_cors_", TransferPath::Digests),
    ("__s4_bucket_versioning_", TransferPath::Digests),
    ("__s4_bucket_lifecycle_", TransferPath::Digests),
    ("__s4_bucket_policy_", TransferPath::Digests),
    ("__s4_bucket_object_lock_", TransferPath::Digests),
    ("__s4_bucket_encryption_", TransferPath::Digests),
    ("__s4_iam_v2_", TransferPath::Digests),
    ("__s4_ec_v1/tiering/", TransferPath::SystemRecords),
    ("__s4_ec_v1/pools/", TransferPath::SystemRecords),
    ("__s4_pool_generation_v1/", TransferPath::SystemRecords),
    (
        "__s4_ec_v1/manifests/",
        TransferPath::Stays(StayReason::OnItsErasureSet),
    ),
    (
        "__s4_ec_v1/shards/",
        TransferPath::Stays(StayReason::OnItsErasureSet),
    ),
    (
        "__s4_ec_v1/small/",
        TransferPath::Stays(StayReason::OnItsErasureSet),
    ),
    ("__s4_iam_node_", TransferPath::Stays(StayReason::NodeLocal)),
    (
        "__s4_iam_user_",
        TransferPath::Stays(StayReason::EarlierRelease),
    ),
    (
        "__s4_iam_username_",
        TransferPath::Stays(StayReason::EarlierRelease),
    ),
    (
        "__s4_iam_access_key_",
        TransferPath::Stays(StayReason::EarlierRelease),
    ),
    ("__s4_iam_", TransferPath::Stays(StayReason::EarlierRelease)),
    (
        "__s4_versions_",
        TransferPath::Stays(StayReason::EarlierRelease),
    ),
];

/// How the move finds `bucket/key`, `None` for a key of `__system__` no
/// family describes.
pub fn path_of(bucket: &str, key: &str) -> Option<TransferPath> {
    if bucket != SYSTEM_BUCKET {
        return Some(TransferPath::Digests);
    }
    let path = SYSTEM_FAMILIES
        .iter()
        .find(|(prefix, _)| key.starts_with(prefix))
        .map(|(_, path)| *path)?;
    // The digests decide which keys they hold; the families must agree.
    debug_assert_eq!(
        path == TransferPath::Digests,
        is_replicated_key(bucket, key),
        "{key}"
    );
    Some(path)
}

/// Whether the move lists `key` of `__system__` as a record without digests.
pub fn is_system_record(key: &str) -> bool {
    SYSTEM_RECORD_PREFIXES.iter().any(|prefix| key.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rows of the plan's table «Что размещается по кольцу пула», each by
    /// a key of its family: what moves is found by the move, what stays is
    /// not. Multipart sessions are no keys; they have a path of their own.
    #[test]
    fn the_families_of_the_plan_table_are_moved_or_stay_as_it_says() {
        use TransferPath::*;
        let rows = [
            ("photos", "a/b.jpg", Digests),
            (SYSTEM_BUCKET, "__s4_bucket_marker_photos", Digests),
            (SYSTEM_BUCKET, "__s4_bucket_cors_photos", Digests),
            (SYSTEM_BUCKET, "__s4_bucket_lifecycle_photos", Digests),
            (SYSTEM_BUCKET, "__s4_bucket_policy_photos", Digests),
            (SYSTEM_BUCKET, "__s4_bucket_encryption_photos", Digests),
            (SYSTEM_BUCKET, "__s4_bucket_versioning_photos", Digests),
            (SYSTEM_BUCKET, "__s4_bucket_object_lock_photos", Digests),
            (SYSTEM_BUCKET, "__s4_ec_v1/tiering/photos", SystemRecords),
            (
                SYSTEM_BUCKET,
                "__s4_ec_v1/pools/0/erasure-sets",
                SystemRecords,
            ),
            (
                SYSTEM_BUCKET,
                "__s4_ec_v1/pools/0/set-layouts/3",
                SystemRecords,
            ),
            (SYSTEM_BUCKET, "__s4_iam_v2_user_7", Digests),
            (SYSTEM_BUCKET, "__s4_pool_generation_v1/0", SystemRecords),
            (
                SYSTEM_BUCKET,
                "__s4_ec_v1/manifests/ab",
                Stays(StayReason::OnItsErasureSet),
            ),
            (
                SYSTEM_BUCKET,
                "__s4_ec_v1/shards/ab/0",
                Stays(StayReason::OnItsErasureSet),
            ),
            (
                SYSTEM_BUCKET,
                "__s4_ec_v1/small/ab",
                Stays(StayReason::OnItsErasureSet),
            ),
            (
                SYSTEM_BUCKET,
                "__s4_versions_photos/a",
                Stays(StayReason::EarlierRelease),
            ),
            (
                SYSTEM_BUCKET,
                "__s4_iam_node_secret_key",
                Stays(StayReason::NodeLocal),
            ),
        ];
        for (bucket, key, path) in rows {
            assert_eq!(path_of(bucket, key), Some(path), "{bucket}/{key}");
            assert_eq!(is_system_record(key), path == SystemRecords, "{key}");
        }
        for config in s4_core::types::BUCKET_CONFIG_KEY_PREFIXES {
            assert_eq!(path_of(SYSTEM_BUCKET, &format!("{config}b")), Some(Digests));
        }
        assert_eq!(
            path_of(SYSTEM_BUCKET, &format!("__s4_{}", "new_family")),
            None
        );
    }

    /// A key name the code builds under `__s4_` that no family describes
    /// fails this test: a new family of `__system__` has to be classified —
    /// moved or staying, with its reason — before it ships. The test reads
    /// the workspace's sources for string literals beginning `"__s4_`; a
    /// literal that is itself the beginning of a family's prefix (as
    /// `"__s4_ec_v1/"`) names a namespace, not a family.
    #[test]
    fn every_system_key_the_code_names_has_a_family() {
        // Names tests give records that stand for "some unreplicated record of
        // `__system__`", or keys of user buckets named like S4's own.
        const TEST_NAMES: [&str; 3] = [
            "__s4_ec_manifest_object",
            "__s4_ec_manifest_photos/cat",
            "__s4_internal",
        ];
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut literals = std::collections::BTreeSet::new();
        let mut files = 0;
        let mut pending = vec![root.clone()];
        while let Some(dir) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let name = entry.file_name().to_string_lossy().to_string();
                if path.is_dir() {
                    if !matches!(name.as_str(), "target" | ".git" | "node_modules" | "ideas") {
                        pending.push(path);
                    }
                } else if name.ends_with(".rs") {
                    files += 1;
                    let text = std::fs::read_to_string(&path).unwrap_or_default();
                    for (at, _) in text.match_indices("\"__s4_") {
                        let literal: String = text[at + 1..]
                            .chars()
                            .take_while(|c| {
                                c.is_ascii_alphanumeric() || matches!(c, '_' | '/' | '-')
                            })
                            .collect();
                        literals.insert(literal);
                    }
                }
            }
        }
        assert!(
            files > 100,
            "the workspace sources were not found under {root:?}"
        );
        assert!(literals.len() > 20, "{literals:?}");
        let unclassified: Vec<_> = literals
            .iter()
            .filter(|literal| {
                path_of(SYSTEM_BUCKET, &format!("{literal}x")).is_none()
                    && !SYSTEM_FAMILIES
                        .iter()
                        .any(|(prefix, _)| prefix.starts_with(literal.as_str()))
                    && !TEST_NAMES.contains(&literal.as_str())
            })
            .collect();
        assert!(
            unclassified.is_empty(),
            "keys of __system__ with no family in the move: {unclassified:?}"
        );
    }
}
