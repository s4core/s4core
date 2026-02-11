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

//! Storage engine implementation.

pub mod bitcask;
pub mod dedup;
pub mod engine;
pub mod index;
pub mod recovery;
pub mod version_index;
pub mod versioning;
pub mod volume;

pub use bitcask::BitcaskStorageEngine;
pub use dedup::Deduplicator;
pub use engine::{
    DeleteMarkerEntry, DeleteResult, ListVersionsResult, ObjectVersion, StorageEngine,
};
pub use index::IndexDb;
pub use recovery::recover_index_from_volumes;
pub use version_index::VersionList;
pub use versioning::{generate_version_id, is_null_version, NULL_VERSION_ID};
pub use volume::{VolumeReader, VolumeWriter};
