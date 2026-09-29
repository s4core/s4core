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

//! Volume management for append-only log storage.
//!
//! This module handles writing and reading from append-only volume files.
//! Volumes are rotated when they reach the configured size limit.

use crate::error::StorageError;
use crate::types::BlobHeader;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, Take};

/// Writes data to append-only volume files.
pub struct VolumeWriter {
    current_volume_id: u32,
    current_volume_path: PathBuf,
    current_file: Option<File>,
    current_offset: u64,
    max_volume_size: u64,
    volumes_dir: PathBuf,
    /// Set while a blob is being written, cleared once it is whole.
    ///
    /// A write is a chain of awaits, and a caller can go away at any of them:
    /// an RPC whose client hung up, an HTTP request whose client timed out.
    /// Whatever the dropped write already handed to the file lands there, so
    /// the file cursor ends up past `current_offset`. Found still set, it means
    /// the bytes past `current_offset` belong to no blob, and they are cut off
    /// before the next write. Without that, the next blob would be written at
    /// the cursor while its offset is reported as `current_offset`, and every
    /// index record from then on would point into the middle of other data.
    unfinished_write: bool,
}

impl VolumeWriter {
    /// Creates a new volume writer.
    ///
    /// # Arguments
    ///
    /// * `volumes_dir` - Directory where volume files are stored
    /// * `max_volume_size` - Maximum size of a volume file in bytes (default: 1GB)
    pub async fn new(volumes_dir: &Path, max_volume_size: u64) -> Result<Self, StorageError> {
        std::fs::create_dir_all(volumes_dir).map_err(StorageError::Io)?;

        let mut writer = Self {
            current_volume_id: 0,
            current_volume_path: PathBuf::new(),
            current_file: None,
            current_offset: 0,
            max_volume_size,
            volumes_dir: volumes_dir.to_path_buf(),
            unfinished_write: false,
        };

        writer.open_or_create_volume().await?;
        Ok(writer)
    }

    /// Cuts off what a write that never finished left past `current_offset`
    /// and puts the cursor back there. See `unfinished_write`.
    ///
    /// Nothing points into those bytes: `current_offset` moves only after a
    /// blob is whole, and an index record is written only after that.
    async fn discard_unfinished_write(&mut self) -> Result<(), StorageError> {
        if !self.unfinished_write {
            return Ok(());
        }
        if let Some(file) = self.current_file.as_mut() {
            file.set_len(self.current_offset).await.map_err(StorageError::Io)?;
            file.seek(std::io::SeekFrom::Start(self.current_offset))
                .await
                .map_err(StorageError::Io)?;
        }
        self.unfinished_write = false;
        Ok(())
    }

    /// Appends a blob to the current volume.
    ///
    /// # Arguments
    ///
    /// * `header` - Blob header
    /// * `key` - Object key
    /// * `data` - Blob data
    ///
    /// # Returns
    ///
    /// Returns the volume ID and offset where the blob was written.
    pub async fn write_blob(
        &mut self,
        header: &BlobHeader,
        key: &str,
        data: &[u8],
    ) -> Result<(u32, u64), StorageError> {
        self.discard_unfinished_write().await?;

        // Check if we need to rotate to a new volume
        let header_size = header.serialized_size().map_err(|e| {
            StorageError::Serialization(format!("Failed to calculate header size: {}", e))
        })?;
        let total_size = header_size as u64 + header.key_len as u64 + header.blob_len;

        if self.current_offset + total_size > self.max_volume_size {
            self.rotate_volume().await?;
        }

        let offset = self.current_offset;
        let volume_id = self.current_volume_id;

        let file = self.current_file.as_mut().ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Volume file not open",
            ))
        })?;
        self.unfinished_write = true;

        // Write header
        let header_bytes =
            bincode::serialize(header).map_err(|e| StorageError::Serialization(e.to_string()))?;
        file.write_all(&header_bytes).await?;

        // Write key
        file.write_all(key.as_bytes()).await?;

        // Write data
        file.write_all(data).await?;

        // Flush to ensure data is written
        file.flush().await?;

        self.current_offset += header_bytes.len() as u64 + key.len() as u64 + data.len() as u64;
        self.unfinished_write = false;

        Ok((volume_id, offset))
    }

    /// Appends a blob from an `AsyncRead` source with seek-back header update.
    ///
    /// Writes a placeholder header, streams data while computing CRC32
    /// incrementally, then seeks back to overwrite the header with the final CRC.
    ///
    /// Returns `(volume_id, offset, crc32)`.
    pub async fn write_blob_streaming<R: AsyncRead + Unpin + Send>(
        &mut self,
        key: &str,
        content_length: u64,
        reader: R,
        buffer_size: usize,
    ) -> Result<(u32, u64, u32), StorageError> {
        self.discard_unfinished_write().await?;

        // Build a placeholder header (crc will be overwritten after streaming)
        let placeholder_header = BlobHeader::new(key.len() as u32, content_length, 0);
        let header_size = placeholder_header.serialized_size().map_err(|e| {
            StorageError::Serialization(format!("Failed to calculate header size: {}", e))
        })?;
        let total_size = header_size as u64 + key.len() as u64 + content_length;

        if self.current_offset + total_size > self.max_volume_size {
            self.rotate_volume().await?;
        }

        let blob_offset = self.current_offset;
        let volume_id = self.current_volume_id;

        let file = self.current_file.as_mut().ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Volume file not open",
            ))
        })?;
        self.unfinished_write = true;

        // Seek to write position (defensive — should already be at end)
        file.seek(std::io::SeekFrom::Start(blob_offset))
            .await
            .map_err(StorageError::Io)?;

        // Write placeholder header
        let header_bytes = bincode::serialize(&placeholder_header)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        file.write_all(&header_bytes).await?;

        // Write key
        file.write_all(key.as_bytes()).await?;

        // Stream data from reader, computing CRC32 incrementally
        let mut buf_reader = BufReader::with_capacity(buffer_size, reader);
        let mut crc_hasher = crc32fast::Hasher::new();
        let mut bytes_written: u64 = 0;
        let mut buf = vec![0u8; buffer_size];

        let stream_result: Result<(), StorageError> = async {
            loop {
                let n = buf_reader.read(&mut buf).await.map_err(StorageError::Io)?;
                if n == 0 {
                    break;
                }
                crc_hasher.update(&buf[..n]);
                file.write_all(&buf[..n]).await?;
                bytes_written += n as u64;
            }
            Ok(())
        }
        .await;

        if let Err(e) = stream_result {
            // Rollback: truncate the volume back to the original offset to avoid
            // leaving a corrupt partial blob in the volume tail.
            if let Err(truncate_err) = file.set_len(blob_offset).await {
                tracing::error!(
                    "Failed to truncate volume after I/O error (volume_id={}, offset={}): {}",
                    volume_id,
                    blob_offset,
                    truncate_err
                );
            }
            let _ = file.seek(std::io::SeekFrom::Start(blob_offset)).await;
            self.current_offset = blob_offset;
            return Err(e);
        }

        if bytes_written != content_length {
            // Rollback: truncate the volume back to the original offset to avoid
            // leaving a corrupt partial blob that would confuse the compactor.
            if let Err(truncate_err) = file.set_len(blob_offset).await {
                tracing::error!(
                    "Failed to truncate volume after short read (volume_id={}, offset={}): {}",
                    volume_id,
                    blob_offset,
                    truncate_err
                );
            }
            file.seek(std::io::SeekFrom::Start(blob_offset))
                .await
                .map_err(StorageError::Io)?;
            self.current_offset = blob_offset;
            return Err(StorageError::InvalidData(format!(
                "Expected {} bytes but read {}",
                content_length, bytes_written
            )));
        }

        let crc32 = crc_hasher.finalize();

        // Seek back and overwrite header with final CRC
        let final_header = BlobHeader::new(key.len() as u32, content_length, crc32);
        let final_header_bytes = bincode::serialize(&final_header)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        file.seek(std::io::SeekFrom::Start(blob_offset))
            .await
            .map_err(StorageError::Io)?;
        file.write_all(&final_header_bytes).await?;

        // Seek back to end for next write
        let new_offset = blob_offset + header_bytes.len() as u64 + key.len() as u64 + bytes_written;
        file.seek(std::io::SeekFrom::Start(new_offset))
            .await
            .map_err(StorageError::Io)?;

        file.flush().await?;

        self.current_offset = new_offset;
        self.unfinished_write = false;

        Ok((volume_id, blob_offset, crc32))
    }

    /// Rotates to a new volume file.
    async fn rotate_volume(&mut self) -> Result<(), StorageError> {
        if let Some(file) = self.current_file.take() {
            file.sync_all().await?;
        }

        self.current_volume_id += 1;
        self.current_offset = 0;
        self.open_or_create_volume().await?;
        Ok(())
    }

    /// Opens or creates the current volume file.
    ///
    /// Uses read-write mode (not append) to support seek-back for streaming writes.
    /// The offset is tracked manually and all writes go to `current_offset`.
    async fn open_or_create_volume(&mut self) -> Result<(), StorageError> {
        let volume_filename = format!("volume_{:06}.dat", self.current_volume_id);
        self.current_volume_path = self.volumes_dir.join(&volume_filename);

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.current_volume_path)
            .await
            .map_err(StorageError::Io)?;

        // Get current file size and seek to end
        let metadata = file.metadata().await.map_err(StorageError::Io)?;
        self.current_offset = metadata.len();
        file.seek(std::io::SeekFrom::End(0)).await.map_err(StorageError::Io)?;

        self.current_file = Some(file);
        Ok(())
    }

    /// Gets the current volume ID.
    pub fn current_volume_id(&self) -> u32 {
        self.current_volume_id
    }

    /// Forces a sync of the current volume to disk.
    pub async fn sync(&mut self) -> Result<(), StorageError> {
        if let Some(file) = self.current_file.as_mut() {
            file.sync_all().await.map_err(StorageError::Io)?;
        }
        Ok(())
    }
}

/// Reads data from volume files.
pub struct VolumeReader {
    volumes_dir: PathBuf,
}

impl VolumeReader {
    /// Creates a new volume reader.
    ///
    /// # Arguments
    ///
    /// * `volumes_dir` - Directory where volume files are stored
    pub fn new(volumes_dir: &Path) -> Self {
        Self {
            volumes_dir: volumes_dir.to_path_buf(),
        }
    }

    /// Reads a blob from a volume.
    ///
    /// # Arguments
    ///
    /// * `volume_id` - Volume file ID
    /// * `offset` - Byte offset in the volume file
    ///
    /// # Returns
    ///
    /// Returns the blob header, key, and data.
    pub async fn read_blob(
        &self,
        volume_id: u32,
        offset: u64,
    ) -> Result<(BlobHeader, String, Vec<u8>), StorageError> {
        let volume_filename = format!("volume_{:06}.dat", volume_id);
        let volume_path = self.volumes_dir.join(&volume_filename);

        let mut file = File::open(&volume_path)
            .await
            .map_err(|_e| StorageError::VolumeNotFound { volume_id })?;

        file.seek(std::io::SeekFrom::Start(offset)).await.map_err(StorageError::Io)?;

        // Read header (we need to know the size first, so we'll read a fixed buffer)
        // For now, assume header is at most 1KB
        let mut header_buffer = vec![0u8; 1024];
        let bytes_read = file.read(&mut header_buffer).await.map_err(StorageError::Io)?;
        header_buffer.truncate(bytes_read);

        // Deserialize header to find actual size
        let header: BlobHeader = bincode::deserialize(&header_buffer)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        // Calculate actual serialized header size
        let actual_header_size = bincode::serialized_size(&header)
            .map_err(|e| StorageError::Serialization(e.to_string()))?
            as u64;

        // Reposition file pointer to after the actual header
        // We read up to 1024 bytes, but the actual header is smaller
        file.seek(std::io::SeekFrom::Start(offset + actual_header_size))
            .await
            .map_err(StorageError::Io)?;

        // Read key
        let mut key_bytes = vec![0u8; header.key_len as usize];
        file.read_exact(&mut key_bytes).await.map_err(StorageError::Io)?;
        let key = String::from_utf8(key_bytes)
            .map_err(|e| StorageError::InvalidData(format!("Invalid key encoding: {}", e)))?;

        // Read data
        let mut data = vec![0u8; header.blob_len as usize];
        file.read_exact(&mut data).await.map_err(StorageError::Io)?;

        Ok((header, key, data))
    }

    /// Opens a streaming reader for a blob's data section.
    ///
    /// Returns `(BlobHeader, key, data_reader)` where `data_reader` is a
    /// bounded `AsyncRead` that yields exactly `header.blob_len` bytes
    /// starting from the data section of the blob. No allocation proportional
    /// to blob size is performed.
    ///
    /// For range reads, the caller can further seek/take on the returned reader.
    pub async fn open_blob_stream(
        &self,
        volume_id: u32,
        offset: u64,
    ) -> Result<(BlobHeader, String, Take<File>), StorageError> {
        let volume_filename = format!("volume_{:06}.dat", volume_id);
        let volume_path = self.volumes_dir.join(&volume_filename);

        let mut file = File::open(&volume_path)
            .await
            .map_err(|_e| StorageError::VolumeNotFound { volume_id })?;

        file.seek(std::io::SeekFrom::Start(offset)).await.map_err(StorageError::Io)?;

        // Read header (max 1KB)
        let mut header_buffer = vec![0u8; 1024];
        let bytes_read = file.read(&mut header_buffer).await.map_err(StorageError::Io)?;
        header_buffer.truncate(bytes_read);

        let header: BlobHeader = bincode::deserialize(&header_buffer)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let actual_header_size = bincode::serialized_size(&header)
            .map_err(|e| StorageError::Serialization(e.to_string()))?
            as u64;

        // Seek past header
        file.seek(std::io::SeekFrom::Start(offset + actual_header_size))
            .await
            .map_err(StorageError::Io)?;

        // Read key
        let mut key_bytes = vec![0u8; header.key_len as usize];
        file.read_exact(&mut key_bytes).await.map_err(StorageError::Io)?;
        let key = String::from_utf8(key_bytes)
            .map_err(|e| StorageError::InvalidData(format!("Invalid key encoding: {}", e)))?;

        // Return a bounded reader over exactly the data section
        let data_reader = file.take(header.blob_len);

        Ok((header, key, data_reader))
    }

    /// Opens a streaming reader for a range within a blob's data.
    ///
    /// Seeks directly to `data_offset + range_start` and returns a reader
    /// bounded to `range_len` bytes. Memory usage is O(1) regardless of
    /// object size.
    pub async fn open_blob_range_stream(
        &self,
        volume_id: u32,
        offset: u64,
        range_start: u64,
        range_len: u64,
    ) -> Result<(BlobHeader, Take<File>), StorageError> {
        let volume_filename = format!("volume_{:06}.dat", volume_id);
        let volume_path = self.volumes_dir.join(&volume_filename);

        let mut file = File::open(&volume_path)
            .await
            .map_err(|_e| StorageError::VolumeNotFound { volume_id })?;

        file.seek(std::io::SeekFrom::Start(offset)).await.map_err(StorageError::Io)?;

        // Read header
        let mut header_buffer = vec![0u8; 1024];
        let bytes_read = file.read(&mut header_buffer).await.map_err(StorageError::Io)?;
        header_buffer.truncate(bytes_read);

        let header: BlobHeader = bincode::deserialize(&header_buffer)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let actual_header_size = bincode::serialized_size(&header)
            .map_err(|e| StorageError::Serialization(e.to_string()))?
            as u64;

        // Seek to: blob_start + header + key + range_start
        let data_start = offset + actual_header_size + header.key_len as u64;
        file.seek(std::io::SeekFrom::Start(data_start + range_start))
            .await
            .map_err(StorageError::Io)?;

        let data_reader = file.take(range_len);

        Ok((header, data_reader))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlobHeaderFlags;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_volume_writer_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path();

        // Create writer
        let mut writer = VolumeWriter::new(volumes_dir, 1024 * 1024).await.unwrap();

        // Write a blob
        let key = "test";
        let data = b"hello world";
        let header = BlobHeader {
            crc: 12345,
            timestamp: 1000000,
            key_len: key.len() as u32,
            blob_len: data.len() as u64,
            flags: BlobHeaderFlags::default(),
        };

        let (volume_id, offset) = writer.write_blob(&header, key, data).await.unwrap();
        assert_eq!(volume_id, 0);
        assert_eq!(offset, 0);

        // Sync to ensure data is written
        writer.sync().await.unwrap();

        // Read it back
        let reader = VolumeReader::new(volumes_dir);
        let (read_header, read_key, read_data) = reader.read_blob(volume_id, offset).await.unwrap();

        assert_eq!(read_header.crc, header.crc);
        assert_eq!(read_header.key_len, header.key_len);
        assert_eq!(read_header.blob_len, header.blob_len);
        assert_eq!(read_key, key);
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn test_volume_writer_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path();

        // Create writer with small max size to force rotation
        // Use a very small size (50 bytes) to ensure rotation happens
        let mut writer = VolumeWriter::new(volumes_dir, 50).await.unwrap();

        // Write first blob
        let data1 = b"data1data1";
        let header1 = BlobHeader {
            crc: 1,
            timestamp: 1000,
            key_len: 3,
            blob_len: data1.len() as u64,
            flags: BlobHeaderFlags::default(),
        };
        let (vol1, _) = writer.write_blob(&header1, "key", data1).await.unwrap();
        assert_eq!(vol1, 0);

        // Write second blob that should trigger rotation
        let data2 = b"data2data2";
        let header2 = BlobHeader {
            crc: 2,
            timestamp: 2000,
            key_len: 3,
            blob_len: data2.len() as u64,
            flags: BlobHeaderFlags::default(),
        };
        let (vol2, _) = writer.write_blob(&header2, "key", data2).await.unwrap();
        assert_eq!(vol2, 1); // Should be in new volume

        assert_eq!(writer.current_volume_id(), 1);
    }

    fn header_for(key: &str, data: &[u8]) -> BlobHeader {
        BlobHeader::new(key.len() as u32, data.len() as u64, crc32fast::hash(data))
    }

    /// Writes `count` blobs and reads each back from the location the writer
    /// reported for it.
    async fn assert_later_writes_read_back(writer: &mut VolumeWriter, dir: &Path, count: usize) {
        let reader = VolumeReader::new(dir);
        for index in 0..count {
            let key = format!("after-{index}");
            let data: Vec<u8> = (0..4096u32).map(|i| (i * 7 + index as u32) as u8).collect();
            let (volume_id, offset) =
                writer.write_blob(&header_for(&key, &data), &key, &data).await.unwrap();
            let (_, read_key, read_data) = reader.read_blob(volume_id, offset).await.unwrap();
            assert_eq!(
                read_key, key,
                "blob {index} is not where the writer said it put it"
            );
            assert_eq!(read_data, data);
        }

        // The volume is still one blob after another up to its end, which is
        // what recovery from volumes and the compactor walk: nothing of the
        // dropped write is left between the blobs.
        let volume = std::fs::read(dir.join("volume_000000.dat")).unwrap();
        let mut position = 0usize;
        let mut keys = Vec::new();
        while position < volume.len() {
            let header: BlobHeader = bincode::deserialize(&volume[position..]).unwrap();
            let header_len = header.serialized_size().unwrap();
            let key_start = position + header_len;
            let key_end = key_start + header.key_len as usize;
            keys.push(String::from_utf8(volume[key_start..key_end].to_vec()).unwrap());
            position = key_end + header.blob_len as usize;
        }
        assert_eq!(
            position,
            volume.len(),
            "the last blob must end where the volume ends"
        );
        let expected: Vec<String> = (0..count).map(|index| format!("after-{index}")).collect();
        assert_eq!(keys[keys.len() - count..], expected[..]);
        assert!(
            keys.iter().all(|key| key != "stalled"),
            "the dropped upload is still in the volume: {keys:?}"
        );
    }

    /// A write whose caller goes away is a write dropped at an await: an RPC
    /// whose client hung up, an HTTP request whose client timed out. What it
    /// already handed to the file lands there, and the blobs written after it
    /// must still be found at the offsets the writer reports for them.
    #[tokio::test]
    async fn a_write_dropped_midway_does_not_move_later_blobs_away_from_their_offsets() {
        use futures::FutureExt;

        let temp_dir = TempDir::new().unwrap();
        let mut writer = VolumeWriter::new(temp_dir.path(), 64 * 1024 * 1024).await.unwrap();
        let key = "dropped";
        let data = vec![0xE2u8; 256 * 1024];

        let mut dropped = false;
        for _ in 0..100 {
            // Polled once and dropped when it is not finished by then.
            if writer.write_blob(&header_for(key, &data), key, &data).now_or_never().is_none() {
                dropped = true;
                break;
            }
        }
        assert!(dropped, "no write stayed pending long enough to be dropped");

        assert_later_writes_read_back(&mut writer, temp_dir.path(), 3).await;
    }

    /// A source that yields some bytes and then never finishes, the way a
    /// client that stalls mid-upload does.
    struct StallingReader {
        remaining: usize,
    }

    impl AsyncRead for StallingReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if self.remaining == 0 {
                return std::task::Poll::Pending;
            }
            let n = self.remaining.min(buf.remaining());
            buf.initialize_unfilled_to(n).fill(0xE2);
            buf.advance(n);
            self.remaining -= n;
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn a_streaming_write_dropped_midway_does_not_move_later_blobs_away_from_their_offsets() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = VolumeWriter::new(temp_dir.path(), 64 * 1024 * 1024).await.unwrap();

        // Runs until the source stalls with part of the body written, and is
        // dropped there.
        let source = StallingReader { remaining: 300_000 };
        let stalled = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            writer.write_blob_streaming("stalled", 1024 * 1024, source, 64 * 1024),
        )
        .await;
        assert!(stalled.is_err(), "a stalled upload must not complete");

        assert_later_writes_read_back(&mut writer, temp_dir.path(), 3).await;
    }

    #[tokio::test]
    async fn test_volume_reader_nonexistent_volume() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path();

        let reader = VolumeReader::new(volumes_dir);
        let result = reader.read_blob(999, 0).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            StorageError::VolumeNotFound { volume_id } => assert_eq!(volume_id, 999),
            _ => panic!("Expected VolumeNotFound error"),
        }
    }
}
