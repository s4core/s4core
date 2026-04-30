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

//! Replica-side read handler for quorum reads.
//!
//! Processes `ReadBlobRequest`, `HeadObjectRequest`, and `ListObjectsRequest`
//! from the coordinator. Each handler reads from the local storage engine
//! and returns the result including HLC timestamps for version comparison.

use std::sync::Arc;

use s4_core::{ObjectStream, ReadOptions, StorageEngine};
use tracing::{debug, warn};

use crate::error::ClusterError;
use crate::hlc::HlcTimestamp;
use crate::rpc::proto;

/// Handles read operations on a replica node.
///
/// Shared across all gRPC handler tasks via `Arc`.
pub struct ReplicaReadHandler {
    storage: Arc<dyn StorageEngine>,
    topology_epoch: u64,
}

impl ReplicaReadHandler {
    /// Create a new replica read handler.
    pub fn new(storage: Arc<dyn StorageEngine>, topology_epoch: u64) -> Self {
        Self {
            storage,
            topology_epoch,
        }
    }

    /// Handle a `ReadBlobRequest` — return full object data + metadata.
    pub async fn handle_read(
        &self,
        request: &proto::ReadBlobRequest,
    ) -> Result<proto::ReadBlobResponse, ClusterError> {
        if let Some(err) = self.check_read_epoch(request.topology_epoch) {
            return Ok(err);
        }

        let result = self.storage.get_object(&request.bucket, &request.key).await;

        match result {
            Ok((data, record)) => {
                // Use stored HLC values if available, fall back to modified_at
                let hlc = if record.hlc_timestamp > 0 {
                    HlcTimestamp {
                        wall_time: record.hlc_timestamp,
                        logical: record.hlc_logical,
                        node_id: record.origin_node_id,
                    }
                } else {
                    HlcTimestamp {
                        wall_time: record.modified_at / 1_000_000, // nanos → millis
                        logical: 0,
                        node_id: 0,
                    }
                };

                debug!(
                    bucket = %request.bucket,
                    key = %request.key,
                    size = data.len(),
                    "replica read completed"
                );

                Ok(proto::ReadBlobResponse {
                    success: true,
                    error: String::new(),
                    data,
                    content_hash: record.content_hash.to_vec(),
                    content_type: record.content_type,
                    metadata: record.metadata,
                    hlc: Some(hlc.to_proto()),
                    version_id: record.version_id,
                    content_length: record.size,
                    epoch_mismatch: false,
                })
            }
            Err(e) => Ok(proto::ReadBlobResponse {
                success: false,
                error: e.to_string(),
                data: Vec::new(),
                content_hash: Vec::new(),
                content_type: String::new(),
                metadata: Default::default(),
                hlc: None,
                version_id: None,
                content_length: 0,
                epoch_mismatch: false,
            }),
        }
    }

    /// Open a streaming object reader for `ReadBlobStream`.
    pub async fn open_read_stream(
        &self,
        request: &proto::ReadBlobRequest,
    ) -> Result<ObjectStream, ClusterError> {
        if request.topology_epoch != 0 && request.topology_epoch != self.topology_epoch {
            return Err(ClusterError::EpochMismatch {
                coordinator: request.topology_epoch,
                replica: self.topology_epoch,
            });
        }

        let range = match (request.range_start, request.range_end) {
            (Some(start), Some(end)) => Some((start, end)),
            (None, None) => None,
            _ => {
                return Err(ClusterError::Storage(
                    "range_start and range_end must be provided together".to_string(),
                ));
            }
        };

        self.storage
            .open_object_stream(&request.bucket, &request.key, ReadOptions { range })
            .await
            .map_err(|e| match e {
                s4_core::StorageError::ObjectNotFound { .. } => ClusterError::ObjectNotFound {
                    bucket: request.bucket.clone(),
                    key: request.key.clone(),
                },
                other => ClusterError::Storage(other.to_string()),
            })
    }

    /// Handle a `HeadObjectRequest` — return metadata without object data.
    pub async fn handle_head(
        &self,
        request: &proto::HeadObjectRequest,
    ) -> Result<proto::HeadObjectResponse, ClusterError> {
        if let Some(err) = self.check_head_epoch(request.topology_epoch) {
            return Ok(err);
        }

        let result = self.storage.head_object(&request.bucket, &request.key).await;

        match result {
            Ok(record) => {
                // Use stored HLC values if available, fall back to modified_at
                let hlc = if record.hlc_timestamp > 0 {
                    HlcTimestamp {
                        wall_time: record.hlc_timestamp,
                        logical: record.hlc_logical,
                        node_id: record.origin_node_id,
                    }
                } else {
                    HlcTimestamp {
                        wall_time: record.modified_at / 1_000_000, // nanos → millis
                        logical: 0,
                        node_id: 0,
                    }
                };

                debug!(
                    bucket = %request.bucket,
                    key = %request.key,
                    etag = %record.etag,
                    "replica head completed"
                );

                Ok(proto::HeadObjectResponse {
                    exists: true,
                    error: String::new(),
                    content_type: record.content_type,
                    content_length: record.size,
                    metadata: record.metadata,
                    hlc: Some(hlc.to_proto()),
                    version_id: record.version_id,
                    etag: record.etag,
                    epoch_mismatch: false,
                })
            }
            Err(_) => Ok(proto::HeadObjectResponse {
                exists: false,
                error: String::new(),
                content_type: String::new(),
                content_length: 0,
                metadata: Default::default(),
                hlc: None,
                version_id: None,
                etag: String::new(),
                epoch_mismatch: false,
            }),
        }
    }

    /// Handle a `ListObjectsRequest` — return objects matching prefix.
    pub async fn handle_list(
        &self,
        request: &proto::ListObjectsRequest,
    ) -> Result<proto::ListObjectsResponse, ClusterError> {
        if request.topology_epoch != 0 && request.topology_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch = request.topology_epoch,
                "epoch mismatch on replica list"
            );
            return Ok(proto::ListObjectsResponse {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_continuation_token: String::new(),
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request.topology_epoch
                ),
                epoch_mismatch: true,
            });
        }

        let max_keys = if request.max_keys == 0 {
            1000
        } else {
            request.max_keys as usize
        };

        let result = if request.continuation_token.is_empty() {
            self.storage.list_objects(&request.bucket, &request.prefix, max_keys + 1).await
        } else {
            self.storage
                .list_objects_after(
                    &request.bucket,
                    &request.prefix,
                    &request.continuation_token,
                    max_keys + 1,
                )
                .await
        };

        match result {
            Ok(entries) => {
                let is_truncated = entries.len() > max_keys;
                let take_count = entries.len().min(max_keys);

                let mut objects = Vec::with_capacity(take_count);
                let mut next_token = String::new();

                for (i, (key, record)) in entries.into_iter().enumerate() {
                    if i >= max_keys {
                        break;
                    }
                    if i == max_keys - 1 && is_truncated {
                        next_token = key.clone();
                    }
                    let hlc = if record.hlc_timestamp > 0 {
                        HlcTimestamp {
                            wall_time: record.hlc_timestamp,
                            logical: record.hlc_logical,
                            node_id: record.origin_node_id,
                        }
                    } else {
                        HlcTimestamp {
                            wall_time: record.modified_at / 1_000_000, // nanos → millis
                            logical: 0,
                            node_id: 0,
                        }
                    };
                    objects.push(proto::ObjectInfo {
                        key,
                        size: record.size,
                        content_type: record.content_type,
                        etag: record.etag,
                        hlc: Some(hlc.to_proto()),
                        version_id: record.version_id,
                    });
                }

                Ok(proto::ListObjectsResponse {
                    objects,
                    common_prefixes: Vec::new(),
                    is_truncated,
                    next_continuation_token: next_token,
                    error: String::new(),
                    epoch_mismatch: false,
                })
            }
            Err(e) => Ok(proto::ListObjectsResponse {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_continuation_token: String::new(),
                error: e.to_string(),
                epoch_mismatch: false,
            }),
        }
    }

    fn check_read_epoch(&self, request_epoch: u64) -> Option<proto::ReadBlobResponse> {
        if request_epoch != 0 && request_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch, "epoch mismatch on replica read"
            );
            return Some(proto::ReadBlobResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request_epoch
                ),
                data: Vec::new(),
                content_hash: Vec::new(),
                content_type: String::new(),
                metadata: Default::default(),
                hlc: None,
                version_id: None,
                content_length: 0,
                epoch_mismatch: true,
            });
        }
        None
    }

    fn check_head_epoch(&self, request_epoch: u64) -> Option<proto::HeadObjectResponse> {
        if request_epoch != 0 && request_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch, "epoch mismatch on replica head"
            );
            return Some(proto::HeadObjectResponse {
                exists: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request_epoch
                ),
                content_type: String::new(),
                content_length: 0,
                metadata: Default::default(),
                hlc: None,
                version_id: None,
                etag: String::new(),
                epoch_mismatch: true,
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct MockStorage {
        data: HashMap<String, (Vec<u8>, s4_core::IndexRecord)>,
    }

    impl MockStorage {
        fn with_object(key: &str, data: &[u8], etag: &str) -> Self {
            let mut map = HashMap::new();
            let record = s4_core::IndexRecord {
                file_id: 1,
                offset: 0,
                size: data.len() as u64,
                etag: etag.to_string(),
                content_type: "text/plain".to_string(),
                metadata: HashMap::new(),
                content_hash: [0u8; 32],
                created_at: 1000,
                modified_at: 2000,
                version_id: None,
                is_delete_marker: false,
                retention_mode: None,
                retain_until_timestamp: None,
                legal_hold: false,
                layout: None,
                pool_id: 0,
                hlc_timestamp: 0,
                hlc_logical: 0,
                origin_node_id: 0,
                operation_id: 0,
            };
            map.insert(format!("bucket/{key}"), (data.to_vec(), record));
            Self { data: map }
        }

        fn empty() -> Self {
            Self {
                data: HashMap::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl StorageEngine for MockStorage {
        async fn put_object(
            &self,
            _b: &str,
            _k: &str,
            _d: &[u8],
            _ct: &str,
            _m: &HashMap<String, String>,
        ) -> Result<String, s4_core::StorageError> {
            Ok("\"mock-etag\"".into())
        }
        async fn get_object(
            &self,
            bucket: &str,
            key: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            self.data.get(&format!("{bucket}/{key}")).cloned().ok_or(
                s4_core::StorageError::ObjectNotFound {
                    key: key.to_string(),
                },
            )
        }
        async fn delete_object(&self, _b: &str, _k: &str) -> Result<(), s4_core::StorageError> {
            Ok(())
        }
        async fn head_object(
            &self,
            bucket: &str,
            key: &str,
        ) -> Result<s4_core::IndexRecord, s4_core::StorageError> {
            self.data.get(&format!("{bucket}/{key}")).map(|(_, r)| r.clone()).ok_or(
                s4_core::StorageError::ObjectNotFound {
                    key: key.to_string(),
                },
            )
        }
        async fn list_objects(
            &self,
            _b: &str,
            _p: &str,
            _m: usize,
        ) -> Result<Vec<(String, s4_core::IndexRecord)>, s4_core::StorageError> {
            Ok(self.data.iter().map(|(k, (_, r))| (k.clone(), r.clone())).collect())
        }
        async fn list_objects_after(
            &self,
            _b: &str,
            _p: &str,
            _s: &str,
            _m: usize,
        ) -> Result<Vec<(String, s4_core::IndexRecord)>, s4_core::StorageError> {
            Ok(vec![])
        }
        async fn put_object_versioned(
            &self,
            _b: &str,
            _k: &str,
            _d: &[u8],
            _ct: &str,
            _m: &HashMap<String, String>,
            _vs: s4_core::VersioningStatus,
        ) -> Result<(String, Option<String>), s4_core::StorageError> {
            Ok(("\"mock-etag\"".into(), None))
        }
        async fn get_object_version(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn delete_object_versioned(
            &self,
            _b: &str,
            _k: &str,
            _v: Option<&str>,
            _vs: s4_core::VersioningStatus,
        ) -> Result<s4_core::DeleteResult, s4_core::StorageError> {
            Ok(s4_core::DeleteResult {
                delete_marker: false,
                version_id: None,
            })
        }
        async fn head_object_version(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
        ) -> Result<s4_core::IndexRecord, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn list_object_versions(
            &self,
            _b: &str,
            _p: &str,
            _km: Option<&str>,
            _vm: Option<&str>,
            _m: usize,
        ) -> Result<s4_core::ListVersionsResult, s4_core::StorageError> {
            Ok(Default::default())
        }
        #[allow(clippy::too_many_arguments)]
        async fn put_object_with_retention(
            &self,
            _b: &str,
            _k: &str,
            _d: &[u8],
            _ct: &str,
            _m: &HashMap<String, String>,
            _vs: s4_core::VersioningStatus,
            _dr: Option<s4_core::types::DefaultRetention>,
        ) -> Result<(String, Option<String>), s4_core::StorageError> {
            Ok(("\"mock-etag\"".into(), None))
        }
        async fn update_object_metadata(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
            _r: s4_core::IndexRecord,
        ) -> Result<(), s4_core::StorageError> {
            Ok(())
        }
        async fn open_object_stream(
            &self,
            _b: &str,
            _k: &str,
            _opts: s4_core::ReadOptions,
        ) -> Result<s4_core::ObjectStream, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn open_object_version_stream(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
            _opts: s4_core::ReadOptions,
        ) -> Result<s4_core::ObjectStream, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn put_object_streaming(
            &self,
            _b: &str,
            _k: &str,
            _r: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
            _cl: u64,
            _ct: &str,
            _m: &HashMap<String, String>,
            _e: &str,
        ) -> Result<s4_core::StreamingPutResult, s4_core::StorageError> {
            Ok(s4_core::StreamingPutResult {
                content_hash: [0u8; 32],
                crc32: 0,
                bytes_written: 0,
                etag: "\"mock-etag\"".into(),
            })
        }
    }

    #[tokio::test]
    async fn handle_read_returns_data() {
        let storage = Arc::new(MockStorage::with_object(
            "hello.txt",
            b"Hello S4!",
            "\"abc\"",
        ));
        let handler = ReplicaReadHandler::new(storage, 1);

        let req = proto::ReadBlobRequest {
            bucket: "bucket".to_string(),
            key: "hello.txt".to_string(),
            version_id: None,
            coordinator_id: Vec::new(),
            operation_id: Vec::new(),
            topology_epoch: 1,
            range_start: None,
            range_end: None,
        };

        let resp = handler.handle_read(&req).await.unwrap();
        assert!(resp.success);
        assert_eq!(resp.data, b"Hello S4!");
        assert_eq!(resp.content_type, "text/plain");
        assert_eq!(resp.content_length, 9);
    }

    #[tokio::test]
    async fn handle_read_not_found() {
        let storage = Arc::new(MockStorage::empty());
        let handler = ReplicaReadHandler::new(storage, 1);

        let req = proto::ReadBlobRequest {
            bucket: "bucket".to_string(),
            key: "missing.txt".to_string(),
            version_id: None,
            coordinator_id: Vec::new(),
            operation_id: Vec::new(),
            topology_epoch: 1,
            range_start: None,
            range_end: None,
        };

        let resp = handler.handle_read(&req).await.unwrap();
        assert!(!resp.success);
    }

    #[tokio::test]
    async fn handle_read_epoch_mismatch() {
        let storage = Arc::new(MockStorage::empty());
        let handler = ReplicaReadHandler::new(storage, 5);

        let req = proto::ReadBlobRequest {
            bucket: "bucket".to_string(),
            key: "k".to_string(),
            version_id: None,
            coordinator_id: Vec::new(),
            operation_id: Vec::new(),
            topology_epoch: 3,
            range_start: None,
            range_end: None,
        };

        let resp = handler.handle_read(&req).await.unwrap();
        assert!(!resp.success);
        assert!(resp.epoch_mismatch);
    }

    #[tokio::test]
    async fn handle_head_returns_metadata() {
        let storage = Arc::new(MockStorage::with_object(
            "hello.txt",
            b"Hello S4!",
            "\"abc\"",
        ));
        let handler = ReplicaReadHandler::new(storage, 1);

        let req = proto::HeadObjectRequest {
            bucket: "bucket".to_string(),
            key: "hello.txt".to_string(),
            version_id: None,
            topology_epoch: 1,
        };

        let resp = handler.handle_head(&req).await.unwrap();
        assert!(resp.exists);
        assert_eq!(resp.etag, "\"abc\"");
        assert_eq!(resp.content_length, 9);
    }

    #[tokio::test]
    async fn handle_head_not_found() {
        let storage = Arc::new(MockStorage::empty());
        let handler = ReplicaReadHandler::new(storage, 1);

        let req = proto::HeadObjectRequest {
            bucket: "bucket".to_string(),
            key: "missing.txt".to_string(),
            version_id: None,
            topology_epoch: 1,
        };

        let resp = handler.handle_head(&req).await.unwrap();
        assert!(!resp.exists);
    }

    #[tokio::test]
    async fn handle_list_returns_objects() {
        let storage = Arc::new(MockStorage::with_object("hello.txt", b"data", "\"e\""));
        let handler = ReplicaReadHandler::new(storage, 1);

        let req = proto::ListObjectsRequest {
            bucket: "bucket".to_string(),
            prefix: String::new(),
            delimiter: String::new(),
            max_keys: 100,
            continuation_token: String::new(),
            topology_epoch: 1,
        };

        let resp = handler.handle_list(&req).await.unwrap();
        assert!(!resp.objects.is_empty());
        assert!(resp.error.is_empty());
    }
}
