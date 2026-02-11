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
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// Writes data to append-only volume files.
pub struct VolumeWriter {
    current_volume_id: u32,
    current_volume_path: PathBuf,
    current_file: Option<File>,
    current_offset: u64,
    max_volume_size: u64,
    volumes_dir: PathBuf,
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
        };

        writer.open_or_create_volume().await?;
        Ok(writer)
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

        Ok((volume_id, offset))
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
    async fn open_or_create_volume(&mut self) -> Result<(), StorageError> {
        let volume_filename = format!("volume_{:06}.dat", self.current_volume_id);
        self.current_volume_path = self.volumes_dir.join(&volume_filename);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.current_volume_path)
            .await
            .map_err(StorageError::Io)?;

        // Get current file size
        let metadata = file.metadata().await.map_err(StorageError::Io)?;
        self.current_offset = metadata.len();

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
