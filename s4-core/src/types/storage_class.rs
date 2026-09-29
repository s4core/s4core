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

//! Internal storage class metadata.
//!
//! This is S4's physical placement class, not the S3 `x-amz-storage-class`
//! header. Keeping it as a reserved metadata key avoids changing the bincode
//! layout of `IndexRecord` while still making bucket/object placement durable.

use serde::{Deserialize, Serialize};

/// Reserved metadata key for S4's physical storage class.
pub const S4_STORAGE_CLASS_METADATA_KEY: &str = "_s4_storage_class";

/// S4 physical storage class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageClass {
    /// Existing RF=3 replicated storage.
    #[default]
    Standard,
    /// Enterprise erasure-coded storage pool.
    ErasureCoded,
}

impl StorageClass {
    /// Metadata value used in `IndexRecord.metadata`.
    pub fn as_metadata_value(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::ErasureCoded => "erasure_coded",
        }
    }

    /// Parse a metadata value.
    pub fn from_metadata_value(value: &str) -> Option<Self> {
        match value {
            "standard" | "STANDARD" => Some(Self::Standard),
            "erasure_coded" | "ERASURE_CODED" | "ec" | "EC" => Some(Self::ErasureCoded),
            _ => None,
        }
    }

    /// Parse `S4_POOL_TYPE`.
    pub fn from_pool_type(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "standard" => Some(Self::Standard),
            "ec" | "erasure_coded" | "erasure-coded" => Some(Self::ErasureCoded),
            _ => None,
        }
    }

    /// True when this class requires Enterprise erasure coding.
    pub fn is_erasure_coded(self) -> bool {
        matches!(self, Self::ErasureCoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_type_parses_standard_and_ec_aliases() {
        assert_eq!(
            StorageClass::from_pool_type("standard"),
            Some(StorageClass::Standard)
        );
        assert_eq!(
            StorageClass::from_pool_type("ec"),
            Some(StorageClass::ErasureCoded)
        );
        assert_eq!(
            StorageClass::from_pool_type("erasure-coded"),
            Some(StorageClass::ErasureCoded)
        );
        assert_eq!(StorageClass::from_pool_type("glacier"), None);
    }

    #[test]
    fn metadata_value_round_trips() {
        for class in [StorageClass::Standard, StorageClass::ErasureCoded] {
            assert_eq!(
                StorageClass::from_metadata_value(class.as_metadata_value()),
                Some(class)
            );
        }
    }
}
