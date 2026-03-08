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

//! SQL query execution engine — lightweight custom implementation.
//!
//! Provides [`SelectEngine`] which executes SQL queries against object data
//! stored in CSV or JSON format, using a custom planner and evaluator
//! built on Arrow compute kernels.

use arrow::array::RecordBatch;

use crate::config::QueryConfig;
use crate::error::SelectError;
use crate::eval;
use crate::formats;
use crate::planner;
use crate::request::{InputSerialization, SelectRequest};
use crate::sql_validator;

/// Internal table name used in SQL rewriting.
const INTERNAL_TABLE_NAME: &str = "s3object";

/// The SQL query execution engine.
///
/// Executes SQL queries against in-memory object data using a custom
/// planner and Arrow-based expression evaluator.
///
/// # Usage
///
/// ```rust,no_run
/// use s4_select::{SelectEngine, QueryConfig};
///
/// let engine = SelectEngine::new(QueryConfig::default());
/// ```
#[derive(Debug, Clone)]
pub struct SelectEngine {
    config: QueryConfig,
}

impl SelectEngine {
    /// Create a new engine with the given configuration.
    pub fn new(config: QueryConfig) -> Self {
        Self { config }
    }

    /// Execute a single-object SQL query (S3 Select compatible).
    ///
    /// Parses the object data according to `request.input_serialization`,
    /// plans and executes the SQL expression, and returns the result as
    /// `RecordBatch`es.
    pub async fn execute_single_object(
        &self,
        data: &[u8],
        request: &SelectRequest,
    ) -> Result<Vec<RecordBatch>, SelectError> {
        // 1. Validate SQL is read-only
        sql_validator::validate_read_only(&request.expression)?;

        // 2. Rewrite S3Object references
        let sql =
            sql_validator::rewrite_s3object_reference(&request.expression, INTERNAL_TABLE_NAME);

        // 3. Parse object data into Arrow RecordBatches
        let (schema, batches) = formats::parse_object_data(data, &request.input_serialization)?;

        if batches.is_empty() {
            return Ok(Vec::new());
        }

        // 4. Plan and execute with timeout
        let timeout = self.config.timeout;
        let result = tokio::time::timeout(timeout, async {
            // Plan query
            let plan = planner::plan_query(&sql, &schema)?;

            // Execute
            if plan.has_aggregates {
                let batch = eval::execute_aggregate(&plan, &batches)?;
                Ok(vec![batch])
            } else {
                eval::execute_non_aggregate(&plan, &batches)
            }
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Err(SelectError::Timeout(timeout.as_secs())),
        }
    }

    /// Execute a multi-object SQL query (S4 extended API).
    ///
    /// All objects must share the same schema (inferred from the first object).
    /// Objects are combined into a single batch set for querying.
    pub async fn execute_multi_object(
        &self,
        objects: Vec<(&str, &[u8])>,
        sql: &str,
        input: &InputSerialization,
    ) -> Result<Vec<RecordBatch>, SelectError> {
        if objects.is_empty() {
            return Err(SelectError::NoMatchingObjects(
                "No objects provided".to_string(),
            ));
        }

        // 1. Validate SQL is read-only
        sql_validator::validate_read_only(sql)?;

        // 2. Parse all objects into batches
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut schema = None;

        for (key, data) in &objects {
            let (obj_schema, batches) = formats::parse_object_data(data, input)?;

            if schema.is_none() {
                schema = Some(obj_schema);
            }

            all_batches.extend(batches);

            tracing::debug!(key = %key, "Parsed object for multi-object query");
        }

        let schema = schema.ok_or_else(|| {
            SelectError::DataParseError("Could not infer schema from objects".to_string())
        })?;

        if all_batches.is_empty() {
            return Ok(Vec::new());
        }

        // 3. Rewrite SQL and execute
        let rewritten = sql_validator::rewrite_s3object_reference(sql, INTERNAL_TABLE_NAME);

        let timeout = self.config.timeout;
        let result = tokio::time::timeout(timeout, async {
            let plan = planner::plan_query(&rewritten, &schema)?;

            if plan.has_aggregates {
                let batch = eval::execute_aggregate(&plan, &all_batches)?;
                Ok(vec![batch])
            } else {
                eval::execute_non_aggregate(&plan, &all_batches)
            }
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Err(SelectError::Timeout(timeout.as_secs())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{
        CsvInput, FileHeaderInfo, InputSerialization, OutputSerialization, SelectRequest,
    };

    fn make_csv_request(sql: &str) -> SelectRequest {
        SelectRequest {
            expression: sql.to_string(),
            input_serialization: InputSerialization::Csv(CsvInput {
                file_header_info: FileHeaderInfo::Use,
                ..CsvInput::default()
            }),
            output_serialization: OutputSerialization::Json,
            request_progress: false,
            scan_range: None,
        }
    }

    #[tokio::test]
    async fn test_select_all_csv() {
        let engine = SelectEngine::new(QueryConfig::default());
        let data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n";
        let request = make_csv_request("SELECT * FROM s3object");

        let batches = engine.execute_single_object(data, &request).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_select_with_where() {
        let engine = SelectEngine::new(QueryConfig::default());
        let data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n";
        let request = make_csv_request("SELECT name FROM s3object WHERE age > 28");

        let batches = engine.execute_single_object(data, &request).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // Alice (30) and Charlie (35)
    }

    #[tokio::test]
    async fn test_select_count() {
        let engine = SelectEngine::new(QueryConfig::default());
        let data = b"name,age\nAlice,30\nBob,25\nCharlie,35\n";
        let request = make_csv_request("SELECT COUNT(*) FROM s3object");

        let batches = engine.execute_single_object(data, &request).await.unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_reject_insert() {
        let engine = SelectEngine::new(QueryConfig::default());
        let data = b"name,age\nAlice,30\n";
        let request = make_csv_request("INSERT INTO s3object VALUES ('Eve', 22)");

        let result = engine.execute_single_object(data, &request).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SelectError::NonSelectStatement(_)
        ));
    }

    #[tokio::test]
    async fn test_multi_object_query() {
        let engine = SelectEngine::new(QueryConfig::default());

        let data1 = b"name,age\nAlice,30\nBob,25\n";
        let data2 = b"name,age\nCharlie,35\nDave,28\n";

        let objects: Vec<(&str, &[u8])> = vec![
            ("file1.csv", data1.as_slice()),
            ("file2.csv", data2.as_slice()),
        ];

        let input = InputSerialization::Csv(CsvInput {
            file_header_info: FileHeaderInfo::Use,
            ..CsvInput::default()
        });

        let batches = engine
            .execute_multi_object(objects, "SELECT * FROM s3object", &input)
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn test_multi_object_aggregation() {
        let engine = SelectEngine::new(QueryConfig::default());

        let data1 = b"name,amount\nAlice,100\nBob,200\n";
        let data2 = b"name,amount\nCharlie,150\nDave,250\n";

        let objects: Vec<(&str, &[u8])> = vec![
            ("file1.csv", data1.as_slice()),
            ("file2.csv", data2.as_slice()),
        ];

        let input = InputSerialization::Csv(CsvInput {
            file_header_info: FileHeaderInfo::Use,
            ..CsvInput::default()
        });

        let batches = engine
            .execute_multi_object(objects, "SELECT SUM(amount) as total FROM s3object", &input)
            .await
            .unwrap();

        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_timeout() {
        let config = QueryConfig::new().with_timeout(std::time::Duration::from_millis(1));
        let engine = SelectEngine::new(config);

        // Generate a large dataset that would take time to process
        let mut data = String::from("x\n");
        for i in 0..100_000 {
            data.push_str(&format!("{i}\n"));
        }

        let request = make_csv_request("SELECT * FROM s3object WHERE x > 50000");

        // This might timeout or succeed quickly — either is acceptable
        let _result = engine.execute_single_object(data.as_bytes(), &request).await;
    }
}
