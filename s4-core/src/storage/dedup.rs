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

//! Content-addressable deduplication engine.

use crate::types::IndexRecord;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Deduplication engine that checks for existing content before writing.
pub struct Deduplicator {
    /// Map from content hash to (volume_id, offset, reference_count) for existing blobs
    content_map: HashMap<[u8; 32], (u32, u64, u32)>,
}

impl Deduplicator {
    /// Creates a new deduplicator.
    pub fn new() -> Self {
        Self {
            content_map: HashMap::new(),
        }
    }

    /// Computes the SHA-256 hash of the given data.
    ///
    /// # Arguments
    ///
    /// * `data` - Data to hash
    ///
    /// # Returns
    ///
    /// Returns the 32-byte SHA-256 hash.
    pub fn compute_hash(data: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().into()
    }

    /// Checks if content with the given hash already exists.
    ///
    /// # Arguments
    ///
    /// * `content_hash` - SHA-256 hash of the content
    ///
    /// # Returns
    ///
    /// Returns Some((volume_id, offset)) if the content exists, None otherwise.
    pub fn check_existing(&self, content_hash: &[u8; 32]) -> Option<(u32, u64)> {
        self.content_map.get(content_hash).map(|(vol_id, offset, _)| (*vol_id, *offset))
    }

    /// Registers a new content hash with its location or increments reference count.
    ///
    /// # Arguments
    ///
    /// * `content_hash` - SHA-256 hash of the content
    /// * `volume_id` - Volume ID where the content is stored
    /// * `offset` - Offset in the volume file
    ///
    /// If the content already exists, increments the reference count.
    /// Otherwise, creates a new entry with reference count of 1.
    pub fn register_content(&mut self, content_hash: [u8; 32], volume_id: u32, offset: u64) {
        self.content_map
            .entry(content_hash)
            .and_modify(|(_, _, ref_count)| *ref_count += 1)
            .or_insert((volume_id, offset, 1));
    }

    /// Decrements reference count for a content hash (when object is deleted).
    ///
    /// # Arguments
    ///
    /// * `content_hash` - SHA-256 hash to unregister
    ///
    /// Decrements the reference count. If it reaches zero, removes the entry.
    /// This ensures that content is only removed when no objects reference it.
    pub fn unregister_content(&mut self, content_hash: &[u8; 32]) {
        if let Some((_, _, ref_count)) = self.content_map.get_mut(content_hash) {
            *ref_count -= 1;
            if *ref_count == 0 {
                self.content_map.remove(content_hash);
            }
        }
    }

    /// Returns the total number of unique content blobs and total references.
    ///
    /// Used for computing deduplication ratio:
    /// - `unique_blobs`: number of distinct content hashes
    /// - `total_references`: sum of all reference counts
    ///
    /// Dedup ratio = 1.0 - (unique_blobs / total_references) when total_references > 0.
    pub fn stats(&self) -> (u64, u64) {
        let unique_blobs = self.content_map.len() as u64;
        let total_references: u64 =
            self.content_map.values().map(|(_, _, ref_count)| u64::from(*ref_count)).sum();
        (unique_blobs, total_references)
    }

    /// Builds the deduplication map from existing index records.
    ///
    /// This should be called at startup to populate the map from the database.
    ///
    /// # Arguments
    ///
    /// * `records` - Iterator over (key, record) pairs from the index
    ///
    /// # Note
    ///
    /// Inline objects (file_id == u32::MAX) are excluded from deduplication
    /// since they store data in metadata and don't follow the same reference mechanism.
    pub fn build_from_index<I>(&mut self, records: I)
    where
        I: Iterator<Item = (String, IndexRecord)>,
    {
        for (_, record) in records {
            // Skip inline objects - they don't participate in deduplication
            // Inline objects have file_id == u32::MAX
            if record.file_id == u32::MAX {
                continue;
            }

            // When building from index, we need to count how many records reference each content
            // For now, we'll set ref_count to 1 and let normal operations manage it
            // In a production system, we'd count references during build
            self.content_map
                .entry(record.content_hash)
                .and_modify(|(_, _, ref_count)| *ref_count += 1)
                .or_insert((record.file_id, record.offset, 1));
        }
    }
}

impl Default for Deduplicator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_hash() {
        let data1 = b"hello world";
        let data2 = b"hello world";
        let data3 = b"different";

        let hash1 = Deduplicator::compute_hash(data1);
        let hash2 = Deduplicator::compute_hash(data2);
        let hash3 = Deduplicator::compute_hash(data3);

        // Same data should produce same hash
        assert_eq!(hash1, hash2);
        // Different data should produce different hash
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_register_and_check() {
        let mut dedup = Deduplicator::new();
        let data = b"test data";
        let hash = Deduplicator::compute_hash(data);

        // Should not exist initially
        assert_eq!(dedup.check_existing(&hash), None);

        // Register content
        dedup.register_content(hash, 1, 100);

        // Should now exist
        assert_eq!(dedup.check_existing(&hash), Some((1, 100)));
    }

    #[test]
    fn test_reference_counting() {
        let mut dedup = Deduplicator::new();
        let data = b"test data";
        let hash = Deduplicator::compute_hash(data);

        // Register first reference
        dedup.register_content(hash, 1, 100);
        assert_eq!(dedup.check_existing(&hash), Some((1, 100)));

        // Register second reference (same content)
        dedup.register_content(hash, 1, 100);
        assert_eq!(dedup.check_existing(&hash), Some((1, 100)));

        // Unregister first reference - should still exist
        dedup.unregister_content(&hash);
        assert_eq!(dedup.check_existing(&hash), Some((1, 100)));

        // Unregister second reference - should be removed
        dedup.unregister_content(&hash);
        assert_eq!(dedup.check_existing(&hash), None);
    }

    #[test]
    fn test_unregister() {
        let mut dedup = Deduplicator::new();
        let data = b"test data";
        let hash = Deduplicator::compute_hash(data);

        dedup.register_content(hash, 1, 100);
        assert_eq!(dedup.check_existing(&hash), Some((1, 100)));

        dedup.unregister_content(&hash);
        assert_eq!(dedup.check_existing(&hash), None);
    }

    #[test]
    fn test_build_from_index_excludes_inline() {
        let mut dedup = Deduplicator::new();

        // Create inline record (file_id == u32::MAX)
        let inline_record = IndexRecord::new(
            u32::MAX,
            0,
            100,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );

        // Create regular record
        let regular_record = IndexRecord::new(
            1,
            200,
            100,
            [1u8; 32],
            "etag2".to_string(),
            "text/plain".to_string(),
        );

        let records = vec![
            ("key1".to_string(), inline_record),
            ("key2".to_string(), regular_record),
        ];

        dedup.build_from_index(records.into_iter());

        // Inline record should not be registered
        assert_eq!(dedup.check_existing(&[0u8; 32]), None);

        // Regular record should be registered
        assert_eq!(dedup.check_existing(&[1u8; 32]), Some((1, 200)));
    }
}
