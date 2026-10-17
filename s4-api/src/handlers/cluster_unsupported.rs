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

//! What cluster mode does not enforce, said as such.
//!
//! In cluster mode objects are written and deleted through the quorums with
//! no versions and no retention checks, so bucket versioning and Object Lock
//! would be promises nothing keeps: a bucket under Object Lock COMPLIANCE
//! would still lose an object to DeleteObject. Every request that turns either
//! on answers `501 NotImplemented` instead of `200`, as S3, MinIO and Ceph
//! RGW answer for a capability they lack.
//!
//! Buckets that got such a configuration before this refusal still hold it on
//! the node that accepted it. [`unenforced_bucket_configs`] finds them, so
//! the operator learns that the protection they counted on did not hold.

use std::sync::{Arc, PoisonError, RwLock};

use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::s3::errors::S3Error;
use crate::server::AppState;

/// Bucket versioning: Put, and CreateBucket with Object Lock, which turns it on.
pub(crate) const VERSIONING_UNSUPPORTED: &str = "Bucket versioning is not supported in cluster \
     mode: objects are written and deleted without versions";

/// Object Lock: bucket configuration, per-object retention and legal hold.
pub(crate) const OBJECT_LOCK_UNSUPPORTED: &str = "Object Lock is not supported in cluster mode: \
     deletes are not checked against retention or legal hold";

/// The headers that ask for Object Lock on an object being written.
const OBJECT_LOCK_HEADERS: [&str; 3] = [
    "x-amz-object-lock-mode",
    "x-amz-object-lock-retain-until-date",
    "x-amz-object-lock-legal-hold",
];

/// `Some(501 NotImplemented)` with `message` in cluster mode, `None` outside
/// it. Asked before anything else, the bucket included: the answer does not
/// depend on the bucket, and needs no quorum.
pub(crate) fn refuse_in_cluster(state: &AppState, message: &str) -> Option<Response> {
    state
        .is_cluster_mode()
        .then(|| S3Error::NotImplemented(message.to_string()).into_response())
}

/// [`refuse_in_cluster`] with the Object Lock message when the request carries
/// any `x-amz-object-lock-*` header of an object write. A request without one
/// goes on.
pub(crate) fn refuse_object_lock_headers_in_cluster(
    state: &AppState,
    headers: &HeaderMap,
) -> Option<Response> {
    if OBJECT_LOCK_HEADERS.iter().any(|name| headers.contains_key(*name)) {
        refuse_in_cluster(state, OBJECT_LOCK_UNSUPPORTED)
    } else {
        None
    }
}

/// A bucket configuration cluster mode keeps and does not enforce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnenforcedConfigKind {
    /// Versioning, `Enabled` or `Suspended`.
    Versioning,
    /// Object Lock, enabled.
    ObjectLock,
}

impl UnenforcedConfigKind {
    /// Every kind, in the order they are reported.
    pub const ALL: [Self; 2] = [Self::Versioning, Self::ObjectLock];

    /// The name of the kind, as the `kind` label of
    /// `s4_bucket_unenforced_configs` and the health answer give it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Versioning => "versioning",
            Self::ObjectLock => "object_lock",
        }
    }

    /// The `__system__` key prefix the configuration is stored under.
    fn key_prefix(self) -> &'static str {
        match self {
            Self::Versioning => s4_core::types::VERSIONING_CONFIG_KEY_PREFIX,
            Self::ObjectLock => s4_core::types::OBJECT_LOCK_CONFIG_KEY_PREFIX,
        }
    }

    /// Whether the stored value turns the capability on.
    fn turned_on(self, value: &[u8]) -> bool {
        let value = String::from_utf8_lossy(value);
        match self {
            Self::Versioning => {
                s4_core::VersioningStatus::from_xml_status(value.trim())
                    != s4_core::VersioningStatus::Unversioned
            }
            Self::ObjectLock => s4_features::object_lock::parse_object_lock_xml(&value)
                .map(|config| config.object_lock_enabled)
                .unwrap_or(false),
        }
    }
}

/// One bucket configuration this node stores and cluster mode does not
/// enforce.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct UnenforcedBucketConfig {
    /// The bucket.
    pub bucket: String,
    /// What it has turned on.
    pub kind: UnenforcedConfigKind,
}

/// The latest count of the configurations this node stores and does not
/// enforce, kept by the node's background count and read by
/// `/api/admin/cluster/health`: a health check costs no scan of the storage.
#[derive(Debug, Clone, Default)]
pub struct UnenforcedConfigs(Arc<RwLock<Vec<UnenforcedBucketConfig>>>);

impl UnenforcedConfigs {
    /// Replaces the count with `found`.
    pub fn set(&self, found: Vec<UnenforcedBucketConfig>) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = found;
    }

    /// The latest count; empty before the first.
    pub fn get(&self) -> Vec<UnenforcedBucketConfig> {
        self.0.read().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

/// Keys read from `__system__` per page while looking for configurations.
const PAGE: usize = 1000;

/// The versioning and Object Lock configurations this node's own storage
/// holds, sorted by bucket and kind. Only this node's: before cluster mode
/// refused them, a configuration was stored on the node that accepted it.
///
/// A configuration that cannot be read is left out and logged; the list is a
/// warning to the operator, not a decision anything is taken on.
pub async fn unenforced_bucket_configs(
    storage: &dyn s4_core::StorageEngine,
) -> Vec<UnenforcedBucketConfig> {
    let mut found = Vec::new();
    for kind in UnenforcedConfigKind::ALL {
        let prefix = kind.key_prefix();
        let mut after: Option<String> = None;
        loop {
            let page = match &after {
                Some(key) => storage.list_objects_after("__system__", prefix, key, PAGE).await,
                None => storage.list_objects("__system__", prefix, PAGE).await,
            };
            let page = match page {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!(
                        kind = kind.as_str(),
                        error = %error,
                        "could not list stored bucket configurations"
                    );
                    break;
                }
            };
            for (key, _) in &page {
                let Some(bucket) = key.strip_prefix(prefix) else {
                    continue;
                };
                match storage.get_object("__system__", key).await {
                    Ok((value, _)) if kind.turned_on(&value) => {
                        found.push(UnenforcedBucketConfig {
                            bucket: bucket.to_string(),
                            kind,
                        })
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(
                        key = %key,
                        error = %error,
                        "could not read a stored bucket configuration"
                    ),
                }
            }
            match page.last() {
                Some((key, _)) if page.len() == PAGE => after = Some(key.clone()),
                _ => break,
            }
        }
    }
    found.sort();
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::storage::BitcaskStorageEngine;
    use s4_core::StorageEngine;
    use std::collections::HashMap;

    async fn storage(dir: &std::path::Path) -> BitcaskStorageEngine {
        BitcaskStorageEngine::new(
            dir.join("volumes"),
            dir.join("metadata_db"),
            10 * 1024 * 1024,
            4096,
            false,
        )
        .await
        .unwrap()
    }

    async fn put(storage: &BitcaskStorageEngine, key: &str, value: &str) {
        storage
            .put_object(
                "__system__",
                key,
                value.as_bytes(),
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }

    /// Versioning `Enabled` or `Suspended` and Object Lock enabled are found,
    /// sorted by bucket; Object Lock stored as disabled, an unreadable value
    /// and the other configurations of the same buckets are not.
    #[tokio::test]
    async fn finds_the_versioning_and_object_lock_this_node_stores() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage(dir.path()).await;
        let lock = |enabled: bool| {
            s4_features::object_lock::object_lock_to_xml(
                &s4_features::object_lock::ObjectLockConfiguration {
                    object_lock_enabled: enabled,
                    default_retention: None,
                },
            )
        };
        put(&storage, "__s4_bucket_versioning_beta", "Enabled").await;
        put(&storage, "__s4_bucket_versioning_alpha", "Suspended").await;
        put(&storage, "__s4_bucket_versioning_gamma", "garbage").await;
        put(&storage, "__s4_bucket_object_lock_beta", &lock(true)).await;
        put(&storage, "__s4_bucket_object_lock_delta", &lock(false)).await;
        put(&storage, "__s4_bucket_object_lock_omega", "<not-xml").await;
        put(&storage, "__s4_bucket_cors_alpha", "<CORSConfiguration/>").await;
        put(&storage, "__s4_bucket_marker_alpha", "1").await;

        let found = unenforced_bucket_configs(&storage).await;
        let found: Vec<(&str, &str)> =
            found.iter().map(|c| (c.bucket.as_str(), c.kind.as_str())).collect();
        assert_eq!(
            found,
            [
                ("alpha", "versioning"),
                ("beta", "versioning"),
                ("beta", "object_lock"),
            ]
        );
    }

    /// A configuration deleted with its bucket is no longer reported.
    #[tokio::test]
    async fn a_deleted_configuration_is_not_reported() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage(dir.path()).await;
        put(&storage, "__s4_bucket_versioning_gone", "Enabled").await;
        storage
            .delete_object("__system__", "__s4_bucket_versioning_gone")
            .await
            .unwrap();
        assert!(unenforced_bucket_configs(&storage).await.is_empty());
    }
}
