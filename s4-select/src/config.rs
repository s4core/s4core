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

//! Configuration for the S4 Select query engine.

use std::time::Duration;

/// Default memory limit per query: 256 MB.
const DEFAULT_MAX_MEMORY_BYTES: usize = 256 * 1024 * 1024;

/// Default query timeout: 60 seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 60;

/// Configuration for SQL query execution.
#[derive(Debug, Clone)]
pub struct QueryConfig {
    /// Maximum memory (in bytes) that a single query may consume.
    /// Enforced via DataFusion's `MemoryPool`.
    pub max_memory_bytes: usize,

    /// Maximum time a query may run before being cancelled.
    pub timeout: Duration,

    /// Optional limit on the number of result rows returned.
    /// `None` means unlimited.
    pub max_result_rows: Option<usize>,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
            max_result_rows: None,
        }
    }
}

impl QueryConfig {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum memory limit.
    pub fn with_max_memory(mut self, bytes: usize) -> Self {
        self.max_memory_bytes = bytes;
        self
    }

    /// Set the query timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum result row count.
    pub fn with_max_result_rows(mut self, max_rows: usize) -> Self {
        self.max_result_rows = Some(max_rows);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = QueryConfig::default();
        assert_eq!(config.max_memory_bytes, 256 * 1024 * 1024);
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert!(config.max_result_rows.is_none());
    }

    #[test]
    fn test_builder_pattern() {
        let config = QueryConfig::new()
            .with_max_memory(512 * 1024 * 1024)
            .with_timeout(Duration::from_secs(120))
            .with_max_result_rows(1000);

        assert_eq!(config.max_memory_bytes, 512 * 1024 * 1024);
        assert_eq!(config.timeout, Duration::from_secs(120));
        assert_eq!(config.max_result_rows, Some(1000));
    }
}
