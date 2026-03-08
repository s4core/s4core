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

//! JSON format parser — converts JSON bytes into Arrow RecordBatches.
//!
//! Supports two JSON modes:
//! - **Lines** (NDJSON): Each line is a separate JSON object.
//! - **Document**: The entire content is a single JSON array or object.

use std::sync::Arc;

use crate::error::SelectError;
use crate::request::{JsonInput, JsonType};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::json::ReaderBuilder;

/// Default batch size for JSON parsing.
const JSON_BATCH_SIZE: usize = 8192;

/// Parse JSON bytes into Arrow RecordBatches.
pub fn parse_json(
    data: &[u8],
    config: &JsonInput,
) -> Result<(SchemaRef, Vec<RecordBatch>), SelectError> {
    match config.json_type {
        JsonType::Lines => parse_json_lines(data),
        JsonType::Document => parse_json_document(data),
    }
}

/// Parse JSON Lines (NDJSON) — each line is a JSON object.
fn parse_json_lines(data: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>), SelectError> {
    // Infer schema from JSON data
    let schema = infer_json_schema(data)?;

    let cursor = std::io::Cursor::new(data);
    let reader = ReaderBuilder::new(schema.clone())
        .with_batch_size(JSON_BATCH_SIZE)
        .build(cursor)
        .map_err(|e| SelectError::DataParseError(format!("JSON reader error: {e}")))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| SelectError::DataParseError(format!("JSON batch error: {e}")))?;
        batches.push(batch);
    }

    Ok((schema, batches))
}

/// Parse a JSON Document — the entire content is a single JSON array or object.
fn parse_json_document(data: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>), SelectError> {
    // The arrow-json reader expects NDJSON. Convert JSON Document to NDJSON:
    // - Array root → each element becomes a line
    // - Object root → single line

    let value: serde_json::Value = serde_json::from_slice(data)
        .map_err(|e| SelectError::DataParseError(format!("Invalid JSON document: {e}")))?;

    let ndjson = match value {
        serde_json::Value::Array(items) => {
            let mut lines = Vec::with_capacity(items.len());
            for item in items {
                lines
                    .push(serde_json::to_string(&item).map_err(|e| {
                        SelectError::DataParseError(format!("JSON serialize: {e}"))
                    })?);
            }
            lines.join("\n")
        }
        obj @ serde_json::Value::Object(_) => serde_json::to_string(&obj)
            .map_err(|e| SelectError::DataParseError(format!("JSON serialize: {e}")))?,
        other => {
            return Err(SelectError::DataParseError(format!(
                "JSON document root must be an array or object, got: {}",
                json_type_name(&other)
            )));
        }
    };

    parse_json_lines(ndjson.as_bytes())
}

/// Infer schema from JSON data by sampling.
fn infer_json_schema(data: &[u8]) -> Result<SchemaRef, SelectError> {
    let mut cursor = std::io::Cursor::new(data);
    let (schema, _count) = arrow::json::reader::infer_json_schema_from_seekable(&mut cursor, None)
        .map_err(|e| SelectError::DataParseError(format!("JSON schema inference failed: {e}")))?;

    Ok(Arc::new(schema))
}

/// Format Arrow RecordBatches as JSON Lines bytes (NDJSON).
pub fn batches_to_json(batches: &[RecordBatch]) -> Result<Vec<u8>, SelectError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| SelectError::Internal(format!("JSON write error: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| SelectError::Internal(format!("JSON finish error: {e}")))?;
    }
    Ok(buf)
}

/// Format Arrow RecordBatches as a JSON array.
pub fn batches_to_json_array(batches: &[RecordBatch]) -> Result<Vec<u8>, SelectError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::json::ArrayWriter::new(&mut buf);
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| SelectError::Internal(format!("JSON write error: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| SelectError::Internal(format!("JSON finish error: {e}")))?;
    }
    Ok(buf)
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::JsonType;

    #[test]
    fn test_parse_json_lines() {
        let data = b"{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n";
        let config = JsonInput {
            json_type: JsonType::Lines,
        };

        let (schema, batches) = parse_json(data, &config).unwrap();
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("age").is_ok());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_parse_json_document_array() {
        let data = b"[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]";
        let config = JsonInput {
            json_type: JsonType::Document,
        };

        let (schema, batches) = parse_json(data, &config).unwrap();
        assert!(schema.field_with_name("name").is_ok());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_parse_json_document_single_object() {
        let data = b"{\"name\":\"Alice\",\"age\":30}";
        let config = JsonInput {
            json_type: JsonType::Document,
        };

        let (schema, batches) = parse_json(data, &config).unwrap();
        assert!(schema.field_with_name("name").is_ok());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn test_batches_to_json() {
        let data = b"{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n";
        let config = JsonInput {
            json_type: JsonType::Lines,
        };

        let (_schema, batches) = parse_json(data, &config).unwrap();
        let json_bytes = batches_to_json(&batches).unwrap();
        let json_str = String::from_utf8(json_bytes).unwrap();
        assert!(json_str.contains("Alice"));
        assert!(json_str.contains("Bob"));
    }
}
