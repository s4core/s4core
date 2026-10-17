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

//! Where a change of IAM is read and written.
//!
//! Every change is made under the lease of IAM changes (plan
//! CLUSTER-IAM-REPLICATION.md, decision 3): [`IamRecordStore::begin`] takes it
//! and the [`IamChange`] it returns reads, checks and writes, until it is
//! finished. A single node's lease is a lock of its own. A cluster node's is
//! granted by a majority of its pool: the change reads what it changes from a
//! majority, under the lease's epoch, and writes to a majority, fenced by it,
//! so a change is confirmed only once most nodes hold it and no two changes
//! interleave (decision 1). Checking credentials never goes through here: it
//! reads the node's own copy ([`super::IamDirectory`]).

use std::collections::HashMap;
use std::sync::Arc;

use s4_core::error::StorageError;
use s4_core::storage::StorageEngine;
use s4_core::types::SYSTEM_BUCKET;
use tokio::sync::{Mutex, OwnedMutexGuard};

use super::error::IamError;

/// Where changes of IAM are made.
#[async_trait::async_trait]
pub trait IamRecordStore: Send + Sync {
    /// Takes the lease of IAM changes for one change.
    ///
    /// # Errors
    ///
    /// `Busy` when another change held it for the whole wait; `Unavailable`
    /// when too few nodes of the pool answered.
    async fn begin(&self) -> Result<Box<dyn IamChange>, IamError>;
}

/// One change of IAM, under the lease. Finish it with [`IamChange::finish`];
/// dropped, its lease runs out by itself.
#[async_trait::async_trait]
pub trait IamChange: Send + Sync {
    /// The epoch of the lease the change is made under; `0` on a single node.
    fn epoch(&self) -> u64;

    /// The record at `key`, as the newest version the store confirms has it;
    /// `None` when there is none or it was deleted.
    ///
    /// # Errors
    ///
    /// `Unavailable` when too few nodes answered to tell; `Storage` on a fault.
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, IamError>;

    /// Every live record under `prefix`, with its key, as `read` reads each.
    ///
    /// # Errors
    ///
    /// As [`Self::read`].
    async fn records(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, IamError>;

    /// Writes `data` as the record at `key`.
    ///
    /// # Errors
    ///
    /// `Unavailable` when too few nodes confirmed the write; it may still
    /// reach the others later, as any write that missed its quorum.
    async fn write(&self, key: &str, data: &[u8]) -> Result<(), IamError>;

    /// Deletes the record at `key`; a record that does not exist is no error.
    ///
    /// # Errors
    ///
    /// `Unavailable` when too few nodes confirmed the delete.
    async fn delete(&self, key: &str) -> Result<(), IamError>;

    /// Releases the lease: the next change may take it at once.
    async fn finish(self: Box<Self>);
}

/// The IAM records in this node's own storage: a single node, whose changes
/// take turns on a lock.
#[derive(Clone)]
pub struct LocalRecordStore {
    storage: Arc<dyn StorageEngine>,
    turn: Arc<Mutex<()>>,
}

impl LocalRecordStore {
    /// The records of `storage`.
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            turn: Arc::new(Mutex::new(())),
        }
    }

    /// The record at `key` in this node's storage.
    pub(crate) async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, IamError> {
        match self.storage.get_object(SYSTEM_BUCKET, key).await {
            Ok((data, _)) => Ok(Some(data)),
            Err(StorageError::ObjectNotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Writes `data` as the record at `key` in this node's storage.
    pub(crate) async fn write(&self, key: &str, data: &[u8]) -> Result<(), IamError> {
        self.storage
            .put_object(
                SYSTEM_BUCKET,
                key,
                data,
                "application/json",
                &HashMap::new(),
            )
            .await?;
        Ok(())
    }

    /// Deletes the record at `key` from this node's storage.
    pub(crate) async fn delete(&self, key: &str) -> Result<(), IamError> {
        match self.storage.delete_object(SYSTEM_BUCKET, key).await {
            Ok(()) | Err(StorageError::ObjectNotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait::async_trait]
impl IamRecordStore for LocalRecordStore {
    async fn begin(&self) -> Result<Box<dyn IamChange>, IamError> {
        let turn = self.turn.clone().lock_owned().await;
        Ok(Box::new(LocalChange {
            store: self.clone(),
            _turn: turn,
        }))
    }
}

/// A change of a single node's IAM, holding its turn.
struct LocalChange {
    store: LocalRecordStore,
    _turn: OwnedMutexGuard<()>,
}

#[async_trait::async_trait]
impl IamChange for LocalChange {
    fn epoch(&self) -> u64 {
        0
    }

    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, IamError> {
        self.store.read(key).await
    }

    async fn records(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, IamError> {
        let mut records = Vec::new();
        for key in super::user_directory::list_keys(&*self.store.storage, prefix).await? {
            if let Some(data) = self.store.read(&key).await? {
                records.push((key, data));
            }
        }
        Ok(records)
    }

    async fn write(&self, key: &str, data: &[u8]) -> Result<(), IamError> {
        self.store.write(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<(), IamError> {
        self.store.delete(key).await
    }

    async fn finish(self: Box<Self>) {}
}
