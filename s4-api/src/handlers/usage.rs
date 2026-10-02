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

//! What the buckets hold: the numbers behind the admin API's bucket statistics
//! and the object counters of `/api/stats`, and the pages of objects the admin
//! API lists.
//!
//! A single node counts its local storage. A cluster node counts its pool:
//! buckets and objects are listed from every node of the pool and merged by
//! key, as S3 listings are, so that each object counts once, at the size of its
//! newest version, whichever node is asked. A listing that a node of the pool
//! did not answer leaves the numbers incomplete, and they say so.
//!
//! Counting the pool lists every object of it. The result is kept for ten
//! seconds and computed once for all the requests that ask meanwhile, so that `/api/stats` — open to anyone, and polled by the web
//! console — cannot make the pool list itself over and over. Creating or
//! deleting a bucket through this node drops the kept result.

use std::sync::Arc;
use std::time::{Duration, Instant};

use s4_cluster::ClusterError;
use s4_core::StorageEngine;

use crate::server::AppState;

/// Keys per listing page: a bucket is counted and listed page by page, never
/// held in memory whole.
pub(crate) const PAGE: usize = 1000;

/// How long a cluster node reuses the usage of its pool.
pub(crate) const POOL_USAGE_TTL: Duration = Duration::from_secs(10);

/// One bucket's objects and their bytes.
#[derive(Debug, Clone)]
pub(crate) struct BucketUsage {
    /// Bucket name.
    pub(crate) name: String,
    /// When the bucket was created, in nanoseconds since the epoch.
    pub(crate) created_at: u64,
    /// Objects a client sees in it.
    pub(crate) objects_count: u64,
    /// Their sizes, before deduplication and replication.
    pub(crate) storage_used_bytes: u64,
}

/// Every bucket's usage, by name.
#[derive(Debug, Clone)]
pub(crate) struct Usage {
    /// The buckets, by name.
    pub(crate) buckets: Vec<BucketUsage>,
    /// False when a node of the pool did not answer a listing: objects only
    /// it holds are missing from the numbers.
    pub(crate) complete: bool,
}

impl Usage {
    /// Objects in all buckets.
    pub(crate) fn objects_count(&self) -> u64 {
        self.buckets.iter().map(|bucket| bucket.objects_count).sum()
    }

    /// Bytes of all objects.
    pub(crate) fn storage_used_bytes(&self) -> u64 {
        self.buckets.iter().map(|bucket| bucket.storage_used_bytes).sum()
    }
}

/// Why the usage or a page of objects could not be read.
#[derive(Debug)]
pub(crate) enum UsageError {
    /// A distributed listing failed.
    Cluster(ClusterError),
    /// Local storage failed.
    Storage(String),
}

/// The usage of the pool a cluster node last counted, and when.
#[derive(Default)]
pub struct UsageCache {
    counted: tokio::sync::Mutex<Option<(Instant, Arc<Usage>)>>,
}

impl UsageCache {
    /// Forgets the kept usage: the next request counts again.
    pub(crate) async fn invalidate(&self) {
        *self.counted.lock().await = None;
    }
}

/// The buckets' usage: on a single node counted now; on a cluster node the
/// pool's, counted at most [`POOL_USAGE_TTL`] ago.
pub(crate) async fn usage(state: &AppState) -> Result<Arc<Usage>, UsageError> {
    if state.list_coordinator.is_none() {
        return count(state).await.map(Arc::new);
    }
    // Held while counting: requests that arrive meanwhile wait for this count
    // and take its result instead of starting their own.
    let mut counted = state.usage_cache.counted.lock().await;
    if let Some((at, usage)) = counted.as_ref() {
        if at.elapsed() < POOL_USAGE_TTL {
            return Ok(usage.clone());
        }
    }
    let usage = Arc::new(count(state).await?);
    *counted = Some((Instant::now(), usage.clone()));
    Ok(usage)
}

/// Moves `after` on to `next`, where the next page starts, if it is past it.
/// A listing that handed back nothing past where it was asked to start would
/// be asked for the same page forever: paging stops instead, and the caller
/// marks what it gathered incomplete.
pub(crate) fn advance(after: &mut String, next: String) -> bool {
    if next.as_str() > after.as_str() {
        *after = next;
        return true;
    }
    tracing::error!(after = %after, next = %next, "a listing did not move past where it started; paging stopped");
    false
}

/// Whether a key is an object a client sees: internal keys and version
/// entries are not.
pub(crate) fn is_object_key(key: &str) -> bool {
    !key.starts_with("__s4_") && !key.contains('#')
}

async fn count(state: &AppState) -> Result<Usage, UsageError> {
    let mut complete = true;
    let mut buckets = Vec::new();
    let marker_prefix = s4_core::types::BUCKET_MARKER_KEY_PREFIX;
    let mut after = String::new();
    loop {
        let page = page(state, "__system__", marker_prefix, &after).await?;
        complete &= page.complete;
        for listed in &page.objects {
            if let Some(name) = listed.key.strip_prefix(marker_prefix) {
                buckets.push(BucketUsage {
                    name: name.to_string(),
                    // A marker is written once, when its bucket is created.
                    created_at: listed.modified_at,
                    objects_count: 0,
                    storage_used_bytes: 0,
                });
            }
        }
        match page.last_key {
            Some(last) if page.more => {
                if !advance(&mut after, last) {
                    complete = false;
                    break;
                }
            }
            _ => break,
        }
    }

    for bucket in &mut buckets {
        let mut after = String::new();
        loop {
            let page = page(state, &bucket.name, "", &after).await?;
            complete &= page.complete;
            for listed in page.objects.iter().filter(|listed| is_object_key(&listed.key)) {
                bucket.objects_count += 1;
                bucket.storage_used_bytes += listed.size;
            }
            match page.last_key {
                Some(last) if page.more => {
                    if !advance(&mut after, last) {
                        complete = false;
                        break;
                    }
                }
                _ => break,
            }
        }
    }
    Ok(Usage { buckets, complete })
}

/// One object as a listing shows it.
#[derive(Debug, Clone)]
pub(crate) struct Listed {
    /// Full key.
    pub(crate) key: String,
    /// Size in bytes.
    pub(crate) size: u64,
    /// MIME type.
    pub(crate) content_type: String,
    /// ETag, without quotes.
    pub(crate) etag: String,
    /// Last modification, in nanoseconds since the epoch.
    pub(crate) modified_at: u64,
}

/// A page of a bucket's live objects under a prefix, after a key.
#[derive(Debug)]
pub(crate) struct Page {
    /// The objects, by key; delete markers are not among them.
    pub(crate) objects: Vec<Listed>,
    /// The last key the page covered, delete markers included: where the next
    /// page starts.
    pub(crate) last_key: Option<String>,
    /// Whether keys may follow this page.
    pub(crate) more: bool,
    /// False when a node of the pool did not answer.
    pub(crate) complete: bool,
}

/// Up to [`PAGE`] keys of `bucket` under `prefix` that sort after `after`
/// (`""` for the first page): from local storage on a single node, merged
/// from every node of the pool on a cluster node.
pub(crate) async fn page(
    state: &AppState,
    bucket: &str,
    prefix: &str,
    after: &str,
) -> Result<Page, UsageError> {
    if let Some(ref list_coord) = state.list_coordinator {
        let start = (!after.is_empty()).then_some(after);
        let listed = list_coord
            .list(bucket, prefix, PAGE, start)
            .await
            .map_err(UsageError::Cluster)?;
        return Ok(Page {
            last_key: listed.entries.last().map(|entry| entry.key.clone()),
            more: listed.is_truncated,
            complete: listed.nodes_responded == listed.nodes_queried,
            objects: listed
                .entries
                .into_iter()
                .map(|entry| Listed {
                    key: entry.key,
                    size: entry.size,
                    content_type: entry.content_type,
                    etag: entry.etag,
                    // millis → nanos, as the records of local storage hold it
                    modified_at: entry.hlc.wall_time.saturating_mul(1_000_000),
                })
                .collect(),
        });
    }

    let records = state
        .storage
        .read()
        .await
        .list_objects_after(bucket, prefix, after, PAGE)
        .await
        .map_err(|e| UsageError::Storage(format!("Failed to list objects in '{bucket}': {e}")))?;
    Ok(Page {
        last_key: records.last().map(|(key, _)| key.clone()),
        more: records.len() == PAGE,
        complete: true,
        objects: records
            .into_iter()
            .filter(|(_, record)| !record.is_delete_marker)
            .map(|(key, record)| Listed {
                key,
                size: record.size,
                content_type: record.content_type,
                etag: record.etag,
                modified_at: record.modified_at,
            })
            .collect(),
    })
}
