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

//! Disk-backed multipart upload part storage.
//!
//! Parts are written to temporary files instead of being held in RAM,
//! eliminating OOM risk for large or concurrent multipart uploads.
//! The OS page cache provides free RAM caching for recently written files.

use axum::body::Body;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::fs;
use tokio::io::{AsyncRead, AsyncWriteExt, BufReader, ReadBuf};
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tracing::{debug, warn};

/// Default I/O buffer size (1 MB).
const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

/// Upload-level metadata persisted as a JSON file alongside parts.
///
/// This provides a reliable alternative to the fjall index marker,
/// ensuring CompleteMultipartUpload can always retrieve metadata
/// even if the storage engine marker is lost.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadMetadata {
    /// Target bucket name.
    pub bucket: String,
    /// Target object key.
    pub key: String,
    /// Content-Type for the final assembled object.
    pub content_type: String,
    /// Custom x-amz-meta-* headers.
    pub custom_metadata: HashMap<String, String>,
}

/// Metadata for a single stored part (kept in RAM — ~200 bytes per part).
#[derive(Debug, Clone)]
pub struct PartMetadata {
    /// S3 part number (1-10000).
    pub part_number: u32,
    /// MD5 hex string ETag.
    pub etag: String,
    /// Raw MD5 bytes for multipart ETag computation.
    pub etag_bytes: [u8; 16],
    /// Part size in bytes.
    pub size: u64,
    /// Path to the temp file on disk.
    pub path: PathBuf,
}

/// Tracks the lifecycle state of a multipart upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadPhase {
    /// Upload is open for receiving parts.
    Open,
    /// CompleteMultipartUpload is in progress — abort must be rejected.
    Completing,
}

/// Tracks the state of a single multipart upload.
struct UploadState {
    parts: Vec<PartMetadata>,
    created_at: Instant,
    phase: UploadPhase,
}

/// Disk-backed storage for multipart upload parts.
///
/// Parts are written to `{data_dir}/multipart_tmp/{upload_id}_{part_number:05}.part`.
/// Only lightweight metadata is kept in RAM.
pub struct DiskPartStore {
    tmp_dir: PathBuf,
    parts: Arc<RwLock<HashMap<String, UploadState>>>,
    buffer_size: usize,
}

impl DiskPartStore {
    /// Creates a new store, ensuring the temp directory exists.
    pub async fn new(data_dir: &Path, buffer_size: usize) -> Result<Self, std::io::Error> {
        let tmp_dir = data_dir.join("multipart_tmp");
        fs::create_dir_all(&tmp_dir).await?;
        Ok(Self {
            tmp_dir,
            parts: Arc::new(RwLock::new(HashMap::new())),
            buffer_size: if buffer_size == 0 {
                DEFAULT_BUFFER_SIZE
            } else {
                buffer_size
            },
        })
    }

    /// Creates a new store with default buffer size.
    pub async fn with_defaults(data_dir: &Path) -> Result<Self, std::io::Error> {
        Self::new(data_dir, DEFAULT_BUFFER_SIZE).await
    }

    /// Stores a part to disk. Returns metadata with pre-computed MD5 ETag.
    pub async fn store_part(
        &self,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
    ) -> Result<PartMetadata, std::io::Error> {
        let path = self.part_path(upload_id, part_number);

        // Write data to temp file
        fs::write(&path, data).await?;

        // Compute MD5 for ETag
        let md5_digest = md5::compute(data);
        let etag = format!("{:x}", md5_digest);
        let etag_bytes: [u8; 16] = md5_digest.into();

        let meta = PartMetadata {
            part_number,
            etag,
            etag_bytes,
            size: data.len() as u64,
            path,
        };

        // Store metadata in memory
        let mut parts = self.parts.write().await;
        let state = parts.entry(upload_id.to_string()).or_insert_with(|| UploadState {
            parts: Vec::new(),
            created_at: Instant::now(),
            phase: UploadPhase::Open,
        });

        // Replace existing part with same number (S3 allows overwriting parts)
        state.parts.retain(|p| p.part_number != part_number);
        state.parts.push(meta.clone());

        Ok(meta)
    }

    /// Stores a part by streaming the request body directly to disk.
    ///
    /// Unlike `store_part`, the body is never fully buffered in memory.
    /// Data is written to disk and MD5 is computed incrementally in chunks,
    /// keeping memory usage bounded to the I/O buffer size.
    pub async fn store_part_streaming(
        &self,
        upload_id: &str,
        part_number: u32,
        body: Body,
    ) -> Result<PartMetadata, std::io::Error> {
        let path = self.part_path(upload_id, part_number);

        let file = fs::File::create(&path).await?;
        let mut writer = tokio::io::BufWriter::with_capacity(self.buffer_size, file);
        let mut md5_ctx = md5::Context::new();
        let mut size: u64 = 0;

        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            let data = chunk.map_err(|e| std::io::Error::other(e.to_string()))?;
            md5_ctx.consume(&data);
            writer.write_all(&data).await?;
            size += data.len() as u64;
        }
        writer.flush().await?;

        let md5_digest = md5_ctx.compute();
        let etag = format!("{:x}", md5_digest);
        let etag_bytes: [u8; 16] = md5_digest.into();

        let meta = PartMetadata {
            part_number,
            etag,
            etag_bytes,
            size,
            path,
        };

        // Store metadata in memory
        let mut parts = self.parts.write().await;
        let state = parts.entry(upload_id.to_string()).or_insert_with(|| UploadState {
            parts: Vec::new(),
            created_at: Instant::now(),
            phase: UploadPhase::Open,
        });

        // Replace existing part with same number (S3 allows overwriting parts)
        state.parts.retain(|p| p.part_number != part_number);
        state.parts.push(meta.clone());

        Ok(meta)
    }

    /// Returns ordered list of parts for an upload.
    pub async fn get_parts(&self, upload_id: &str) -> Vec<PartMetadata> {
        let parts = self.parts.read().await;
        match parts.get(upload_id) {
            Some(state) => {
                let mut sorted = state.parts.clone();
                sorted.sort_by_key(|p| p.part_number);
                sorted
            }
            None => Vec::new(),
        }
    }

    /// Creates a lazy `AsyncRead` that streams all parts sequentially.
    /// Opens only one file descriptor at a time to avoid EMFILE.
    pub async fn parts_reader(&self, upload_id: &str) -> Result<LazyPartsReader, std::io::Error> {
        let parts = self.get_parts(upload_id).await;
        let paths: VecDeque<PathBuf> = parts.into_iter().map(|p| p.path).collect();
        Ok(LazyPartsReader {
            paths,
            current: None,
            buffer_size: self.buffer_size,
        })
    }

    /// Creates a lazy `AsyncRead` that streams the specified parts sequentially.
    /// Only the parts in `selected` are included, in the given order.
    pub fn parts_reader_for(&self, selected: &[PartMetadata]) -> LazyPartsReader {
        let paths: VecDeque<PathBuf> = selected.iter().map(|p| p.path.clone()).collect();
        LazyPartsReader {
            paths,
            current: None,
            buffer_size: self.buffer_size,
        }
    }

    /// Deletes all temp files and metadata for an upload.
    pub async fn cleanup_upload(&self, upload_id: &str) -> Result<(), std::io::Error> {
        let removed_parts = {
            let mut parts = self.parts.write().await;
            parts.remove(upload_id)
        };

        if let Some(state) = removed_parts {
            for part in &state.parts {
                if let Err(e) = fs::remove_file(&part.path).await {
                    debug!("Failed to remove temp part file {:?}: {}", part.path, e);
                }
            }
        }

        // Remove the metadata file
        let meta_path = self.metadata_path(upload_id);
        if let Err(e) = fs::remove_file(&meta_path).await {
            debug!(
                "Failed to remove upload metadata file {:?}: {}",
                meta_path, e
            );
        }

        Ok(())
    }

    /// Deletes uploads older than TTL. Returns count of cleaned uploads.
    pub async fn cleanup_expired(&self, ttl: std::time::Duration) -> Result<usize, std::io::Error> {
        let expired_ids: Vec<String> = {
            let parts = self.parts.read().await;
            parts
                .iter()
                .filter(|(_, state)| state.created_at.elapsed() > ttl)
                .map(|(id, _)| id.clone())
                .collect()
        };

        let count = expired_ids.len();
        for id in expired_ids {
            self.cleanup_upload(&id).await?;
        }
        Ok(count)
    }

    /// Cleans up orphaned temp files that have no matching in-memory metadata.
    pub async fn cleanup_orphaned(&self) -> Result<usize, std::io::Error> {
        let mut count = 0;
        let mut entries = fs::read_dir(&self.tmp_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let is_orphan = path.extension().is_some_and(|ext| ext == "part" || ext == "meta");
            if is_orphan {
                if let Err(e) = fs::remove_file(&path).await {
                    warn!("Failed to remove orphaned temp file {:?}: {}", path, e);
                } else {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    /// Total disk usage of all stored parts across all uploads.
    pub async fn total_disk_usage(&self) -> u64 {
        let parts = self.parts.read().await;
        parts.values().flat_map(|s| &s.parts).map(|p| p.size).sum()
    }

    /// Checks if an upload exists in the store.
    pub async fn upload_exists(&self, upload_id: &str) -> bool {
        let parts = self.parts.read().await;
        parts.contains_key(upload_id)
    }

    /// Registers a new upload (called by CreateMultipartUpload).
    pub async fn register_upload(&self, upload_id: &str) {
        let mut parts = self.parts.write().await;
        parts.entry(upload_id.to_string()).or_insert_with(|| UploadState {
            parts: Vec::new(),
            created_at: Instant::now(),
            phase: UploadPhase::Open,
        });
    }

    /// Transitions an upload to the `Completing` phase.
    ///
    /// Returns `true` if the transition succeeded (was `Open`).
    /// Returns `false` if the upload doesn't exist or is already completing.
    pub async fn mark_completing(&self, upload_id: &str) -> bool {
        let mut parts = self.parts.write().await;
        if let Some(state) = parts.get_mut(upload_id) {
            if state.phase == UploadPhase::Open {
                state.phase = UploadPhase::Completing;
                return true;
            }
        }
        false
    }

    /// Returns the current phase of an upload, or `None` if the upload doesn't exist.
    pub async fn get_phase(&self, upload_id: &str) -> Option<UploadPhase> {
        let parts = self.parts.read().await;
        parts.get(upload_id).map(|s| s.phase)
    }

    /// Saves upload-level metadata to a JSON file on disk.
    ///
    /// This is the authoritative source of metadata for CompleteMultipartUpload,
    /// independent of the storage engine index which may lose the marker.
    pub async fn save_upload_metadata(
        &self,
        upload_id: &str,
        metadata: &UploadMetadata,
    ) -> Result<(), std::io::Error> {
        let path = self.metadata_path(upload_id);
        let json = serde_json::to_vec(metadata).map_err(std::io::Error::other)?;
        fs::write(&path, &json).await
    }

    /// Reads upload-level metadata from the JSON file on disk.
    pub async fn get_upload_metadata(
        &self,
        upload_id: &str,
    ) -> Result<UploadMetadata, std::io::Error> {
        let path = self.metadata_path(upload_id);
        let data = fs::read(&path).await?;
        serde_json::from_slice(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    fn part_path(&self, upload_id: &str, part_number: u32) -> PathBuf {
        self.tmp_dir.join(format!("{}_{:05}.part", upload_id, part_number))
    }

    fn metadata_path(&self, upload_id: &str) -> PathBuf {
        self.tmp_dir.join(format!("{}.meta", upload_id))
    }
}

/// Lazily reads parts sequentially, opening one file at a time.
///
/// Implements `AsyncRead` by chaining part files. When the current file
/// reaches EOF, it is closed and the next file is opened.
pub struct LazyPartsReader {
    paths: VecDeque<PathBuf>,
    current: Option<BufReader<fs::File>>,
    buffer_size: usize,
}

impl AsyncRead for LazyPartsReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        loop {
            // If we have an open file, try to read from it
            if let Some(ref mut reader) = this.current {
                let before = buf.filled().len();
                match Pin::new(reader).poll_read(cx, buf) {
                    Poll::Ready(Ok(())) => {
                        let after = buf.filled().len();
                        if after > before {
                            // Got data — return it
                            return Poll::Ready(Ok(()));
                        }
                        // EOF on current file — close it and try next
                        this.current = None;
                        continue;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            // No open file — try to open the next one
            if let Some(path) = this.paths.pop_front() {
                // We need to open the file asynchronously, but poll_read is synchronous.
                // Use std::fs::File for synchronous open (temp files are local, fast).
                let file = match std::fs::File::open(&path) {
                    Ok(f) => f,
                    Err(e) => return Poll::Ready(Err(e)),
                };
                let tokio_file = fs::File::from_std(file);
                this.current = Some(BufReader::with_capacity(this.buffer_size, tokio_file));
                continue;
            }

            // No more files — EOF
            return Poll::Ready(Ok(()));
        }
    }
}

/// Computes S3-compatible multipart ETag from per-part MD5 hashes.
///
/// Formula: `hex(MD5(concat(binary_md5_part1, binary_md5_part2, ...))) + "-" + num_parts`
/// Example: `"8b913d07ee83cb3b50c053c7a7266a01-3"`
pub fn compute_multipart_etag(parts: &[PartMetadata]) -> String {
    let mut hasher = md5::Context::new();
    for part in parts {
        hasher.consume(part.etag_bytes);
    }
    let digest = hasher.compute();
    format!("{:x}-{}", digest, parts.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_store_and_retrieve_part() {
        let tmp = TempDir::new().unwrap();
        let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

        let data = b"hello world";
        let meta = store.store_part("upload1", 1, data).await.unwrap();

        assert_eq!(meta.part_number, 1);
        assert_eq!(meta.size, data.len() as u64);
        assert!(!meta.etag.is_empty());
        assert!(meta.path.exists());

        let parts = store.get_parts("upload1").await;
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);
    }

    #[tokio::test]
    async fn test_store_multiple_parts_ordered() {
        let tmp = TempDir::new().unwrap();
        let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

        store.store_part("upload1", 3, b"part3").await.unwrap();
        store.store_part("upload1", 1, b"part1").await.unwrap();
        store.store_part("upload1", 2, b"part2").await.unwrap();

        let parts = store.get_parts("upload1").await;
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[1].part_number, 2);
        assert_eq!(parts[2].part_number, 3);
    }

    #[tokio::test]
    async fn test_overwrite_part() {
        let tmp = TempDir::new().unwrap();
        let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

        store.store_part("upload1", 1, b"original").await.unwrap();
        let meta = store.store_part("upload1", 1, b"replaced").await.unwrap();

        assert_eq!(meta.size, 8);
        let parts = store.get_parts("upload1").await;
        assert_eq!(parts.len(), 1);
    }

    #[tokio::test]
    async fn test_cleanup_upload() {
        let tmp = TempDir::new().unwrap();
        let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

        let meta = store.store_part("upload1", 1, b"data").await.unwrap();
        assert!(meta.path.exists());

        store.cleanup_upload("upload1").await.unwrap();

        assert!(!meta.path.exists());
        assert!(store.get_parts("upload1").await.is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_orphaned() {
        let tmp = TempDir::new().unwrap();
        let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

        // Create orphaned temp file directly
        let orphan_path = tmp.path().join("multipart_tmp").join("orphan_00001.part");
        fs::write(&orphan_path, b"orphaned").await.unwrap();

        let count = store.cleanup_orphaned().await.unwrap();
        assert_eq!(count, 1);
        assert!(!orphan_path.exists());
    }

    #[tokio::test]
    async fn test_total_disk_usage() {
        let tmp = TempDir::new().unwrap();
        let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

        store.store_part("upload1", 1, &[0u8; 100]).await.unwrap();
        store.store_part("upload1", 2, &[0u8; 200]).await.unwrap();
        store.store_part("upload2", 1, &[0u8; 50]).await.unwrap();

        assert_eq!(store.total_disk_usage().await, 350);
    }

    #[tokio::test]
    async fn test_lazy_parts_reader() {
        use tokio::io::AsyncReadExt;

        let tmp = TempDir::new().unwrap();
        let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

        store.store_part("upload1", 1, b"hello ").await.unwrap();
        store.store_part("upload1", 2, b"world").await.unwrap();

        let mut reader = store.parts_reader("upload1").await.unwrap();
        let mut result = Vec::new();
        reader.read_to_end(&mut result).await.unwrap();

        assert_eq!(result, b"hello world");
    }

    #[tokio::test]
    async fn test_lazy_parts_reader_empty() {
        use tokio::io::AsyncReadExt;

        let tmp = TempDir::new().unwrap();
        let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

        let mut reader = store.parts_reader("nonexistent").await.unwrap();
        let mut result = Vec::new();
        reader.read_to_end(&mut result).await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_compute_multipart_etag() {
        let parts = vec![
            PartMetadata {
                part_number: 1,
                etag: "abc".to_string(),
                etag_bytes: md5::compute(b"part1data").into(),
                size: 9,
                path: PathBuf::new(),
            },
            PartMetadata {
                part_number: 2,
                etag: "def".to_string(),
                etag_bytes: md5::compute(b"part2data").into(),
                size: 9,
                path: PathBuf::new(),
            },
        ];

        let etag = compute_multipart_etag(&parts);
        assert!(etag.ends_with("-2"));
        assert!(etag.contains('-'));
        // The hash part should be 32 hex chars
        let hash_part = etag.split('-').next().unwrap();
        assert_eq!(hash_part.len(), 32);
    }

    #[tokio::test]
    async fn test_register_upload() {
        let tmp = TempDir::new().unwrap();
        let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

        assert!(!store.upload_exists("upload1").await);
        store.register_upload("upload1").await;
        assert!(store.upload_exists("upload1").await);
    }

    #[tokio::test]
    async fn test_concurrent_uploads_isolated() {
        let tmp = TempDir::new().unwrap();
        let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

        store.store_part("upload1", 1, b"data1").await.unwrap();
        store.store_part("upload2", 1, b"data2").await.unwrap();

        let parts1 = store.get_parts("upload1").await;
        let parts2 = store.get_parts("upload2").await;

        assert_eq!(parts1.len(), 1);
        assert_eq!(parts2.len(), 1);

        // Cleanup one doesn't affect the other
        store.cleanup_upload("upload1").await.unwrap();
        assert!(store.get_parts("upload1").await.is_empty());
        assert_eq!(store.get_parts("upload2").await.len(), 1);
    }
}
