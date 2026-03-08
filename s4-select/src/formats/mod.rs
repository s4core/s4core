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

//! Data format parsers for converting stored objects into Arrow RecordBatches.
//!
//! Each sub-module handles a specific format (CSV, JSON, Parquet) and converts
//! raw bytes into Arrow [`RecordBatch`] for SQL execution.

pub mod csv;
pub mod json;

use crate::error::SelectError;
use crate::request::{CsvInput, FileHeaderInfo, InputSerialization, JsonInput};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

/// Supported input data formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputFormat {
    /// CSV (comma-separated values).
    Csv,
    /// JSON (Lines or Document).
    Json,
    /// Apache Parquet columnar format.
    Parquet,
}

/// Supported output result formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputFormat {
    /// CSV output.
    Csv,
    /// JSON output.
    Json,
}

/// Parse raw object bytes into Arrow RecordBatches based on input serialization config.
pub fn parse_object_data(
    data: &[u8],
    input: &InputSerialization,
) -> Result<(SchemaRef, Vec<RecordBatch>), SelectError> {
    match input {
        InputSerialization::Csv(csv_input) => csv::parse_csv(data, csv_input),
        InputSerialization::Json(json_input) => json::parse_json(data, json_input),
        InputSerialization::Parquet => Err(SelectError::UnsupportedFormat(
            "Parquet format is not yet supported in this build".to_string(),
        )),
    }
}

/// Infer the [`InputFormat`] from a content-type string.
pub fn format_from_content_type(content_type: &str) -> Option<InputFormat> {
    let ct = content_type.to_lowercase();
    if ct.contains("csv") || ct.contains("comma-separated") {
        Some(InputFormat::Csv)
    } else if ct.contains("json") {
        Some(InputFormat::Json)
    } else if ct.contains("parquet") || ct.contains("octet-stream") {
        Some(InputFormat::Parquet)
    } else {
        None
    }
}

/// Detect format from file extension in the object key.
pub fn format_from_key(key: &str) -> Option<InputFormat> {
    let lower = key.to_lowercase();
    if lower.ends_with(".csv") || lower.ends_with(".tsv") {
        Some(InputFormat::Csv)
    } else if lower.ends_with(".json") || lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
        Some(InputFormat::Json)
    } else if lower.ends_with(".parquet") || lower.ends_with(".pq") {
        Some(InputFormat::Parquet)
    } else {
        None
    }
}

/// Build a default [`InputSerialization`] for an [`InputFormat`].
pub fn default_input_serialization(format: &InputFormat) -> InputSerialization {
    match format {
        InputFormat::Csv => InputSerialization::Csv(CsvInput {
            file_header_info: FileHeaderInfo::Use,
            ..CsvInput::default()
        }),
        InputFormat::Json => InputSerialization::Json(JsonInput::default()),
        InputFormat::Parquet => InputSerialization::Parquet,
    }
}
