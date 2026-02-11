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

//! Index database wrapper for metadata storage.

use crate::error::StorageError;
use crate::types::IndexRecord;
use redb::{Database, ReadableTable, TableDefinition};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task;

const INDEX_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("index");

/// Wrapper around redb for storing metadata index.
/// Uses a single database connection with mutex for thread-safe access.
pub struct IndexDb {
    db: Arc<Mutex<Database>>,
}

impl IndexDb {
    /// Creates or opens an index database.
    ///
    /// # Arguments
    ///
    /// * `db_path` - Path to the redb database file
    pub fn new(db_path: &Path) -> Result<Self, StorageError> {
        // Ensure parent directory exists
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(StorageError::Io)?;
        }

        // Open or create database
        let db = Database::create(db_path).map_err(|e| StorageError::Database(e.to_string()))?;

        // Initialize table (redb requires a write transaction to create tables)
        let write_txn = db.begin_write().map_err(|e| StorageError::Database(e.to_string()))?;
        {
            let _table = write_txn
                .open_table(INDEX_TABLE)
                .map_err(|e| StorageError::Database(e.to_string()))?;
        }
        write_txn.commit().map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    /// Stores an index record.
    ///
    /// # Arguments
    ///
    /// * `key` - Object key (bucket/path)
    /// * `record` - Index record to store
    pub async fn put(&self, key: &str, record: &IndexRecord) -> Result<(), StorageError> {
        let db = self.db.clone();
        let key = key.to_string();
        let record_bytes =
            bincode::serialize(record).map_err(|e| StorageError::Serialization(e.to_string()))?;

        task::spawn_blocking(move || {
            // Block on acquiring the mutex in the blocking task
            let db_guard = futures::executor::block_on(db.lock());

            let write_txn =
                db_guard.begin_write().map_err(|e| StorageError::Database(e.to_string()))?;
            {
                let mut table = write_txn
                    .open_table(INDEX_TABLE)
                    .map_err(|e| StorageError::Database(e.to_string()))?;
                table
                    .insert(&*key, &record_bytes[..])
                    .map_err(|e| StorageError::Database(e.to_string()))?;
            }
            write_txn.commit().map_err(|e| StorageError::Database(e.to_string()))?;

            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Retrieves an index record.
    ///
    /// # Arguments
    ///
    /// * `key` - Object key
    ///
    /// # Returns
    ///
    /// Returns the index record if found.
    pub async fn get(&self, key: &str) -> Result<Option<IndexRecord>, StorageError> {
        let db = self.db.clone();
        let key = key.to_string();

        task::spawn_blocking(move || {
            let db_guard = futures::executor::block_on(db.lock());

            let read_txn =
                db_guard.begin_read().map_err(|e| StorageError::Database(e.to_string()))?;
            let table = read_txn
                .open_table(INDEX_TABLE)
                .map_err(|e| StorageError::Database(e.to_string()))?;

            // Extract data from AccessGuard before transaction is dropped
            let result = match table.get(&*key) {
                Ok(Some(value)) => {
                    // Copy the data out of the AccessGuard before the transaction ends
                    let data: Vec<u8> = value.value().to_vec();
                    let record: IndexRecord = bincode::deserialize(&data)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    Ok(Some(record))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(StorageError::Database(e.to_string())),
            };
            result
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Deletes an index record.
    ///
    /// # Arguments
    ///
    /// * `key` - Object key
    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let db = self.db.clone();
        let key = key.to_string();

        task::spawn_blocking(move || {
            let db_guard = futures::executor::block_on(db.lock());

            let write_txn =
                db_guard.begin_write().map_err(|e| StorageError::Database(e.to_string()))?;
            {
                let mut table = write_txn
                    .open_table(INDEX_TABLE)
                    .map_err(|e| StorageError::Database(e.to_string()))?;
                table.remove(&*key).map_err(|e| StorageError::Database(e.to_string()))?;
            }
            write_txn.commit().map_err(|e| StorageError::Database(e.to_string()))?;

            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Lists all keys with the given prefix.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Key prefix to filter by
    /// * `max_keys` - Maximum number of keys to return
    ///
    /// # Returns
    ///
    /// Returns a vector of (key, record) pairs.
    pub async fn list(
        &self,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        let db = self.db.clone();
        let prefix = prefix.to_string();

        task::spawn_blocking(move || {
            let db_guard = futures::executor::block_on(db.lock());

            let read_txn =
                db_guard.begin_read().map_err(|e| StorageError::Database(e.to_string()))?;
            let table = read_txn
                .open_table(INDEX_TABLE)
                .map_err(|e| StorageError::Database(e.to_string()))?;

            let mut results = Vec::new();
            for item in table.iter().map_err(|e| StorageError::Database(e.to_string()))? {
                let (key, value) = item.map_err(|e| StorageError::Database(e.to_string()))?;
                let key_str = key.value();

                if key_str.starts_with(&prefix) {
                    // Copy data from AccessGuard before transaction is dropped
                    let data: Vec<u8> = value.value().to_vec();
                    let record: IndexRecord = bincode::deserialize(&data)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    results.push((key_str.to_string(), record));

                    if results.len() >= max_keys {
                        break;
                    }
                }
            }

            Ok(results)
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_index_db_put_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");

        let index_db = IndexDb::new(&db_path).unwrap();

        let record = crate::types::IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );

        index_db.put("bucket/key", &record).await.unwrap();

        let retrieved = index_db.get("bucket/key").await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.file_id, 1);
        assert_eq!(retrieved.offset, 100);
        assert_eq!(retrieved.size, 50);
    }

    #[tokio::test]
    async fn test_index_db_delete() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");

        let index_db = IndexDb::new(&db_path).unwrap();

        let record = crate::types::IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );

        index_db.put("bucket/key", &record).await.unwrap();
        assert!(index_db.get("bucket/key").await.unwrap().is_some());

        index_db.delete("bucket/key").await.unwrap();
        assert!(index_db.get("bucket/key").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_index_db_list() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");

        let index_db = IndexDb::new(&db_path).unwrap();

        let record1 = crate::types::IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag1".to_string(),
            "text/plain".to_string(),
        );
        let record2 = crate::types::IndexRecord::new(
            1,
            200,
            50,
            [1u8; 32],
            "etag2".to_string(),
            "text/plain".to_string(),
        );

        index_db.put("bucket/key1", &record1).await.unwrap();
        index_db.put("bucket/key2", &record2).await.unwrap();
        index_db.put("other/key3", &record1).await.unwrap();

        let results = index_db.list("bucket/", 10).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(k, _)| k == "bucket/key1"));
        assert!(results.iter().any(|(k, _)| k == "bucket/key2"));
    }
}
