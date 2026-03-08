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

//! S3 Select and S4 SQL query handlers.
//!
//! Provides two endpoints:
//! - [`select_object_content`]: AWS S3 Select compatible single-object query
//! - [`bucket_sql_query`]: S4 extended multi-object SQL query

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use s4_core::storage::StorageEngine;
use s4_select::{
    config::QueryConfig,
    engine::SelectEngine,
    formats, multi_object,
    request::{self, MultiObjectSqlRequest},
    response as event_stream,
};

use crate::s3::errors::S3Error;
use crate::server::AppState;

/// Handle S3 SelectObjectContent API.
///
/// **Route**: `POST /{bucket}/{key}?select&select-type=2`
///
/// Parses the XML request body, executes a SQL query against the specified object,
/// and returns results as an S3-compatible binary event stream.
pub async fn select_object_content(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    _headers: HeaderMap,
    body: Bytes,
) -> Response {
    // 1. Parse the XML request body
    let select_request = match request::parse_select_request(&body) {
        Ok(req) => req,
        Err(e) => {
            tracing::warn!(error = %e, "Invalid S3 Select request");
            return S3Error::InvalidRequest(e.to_string()).into_response();
        }
    };

    tracing::debug!(
        bucket = %bucket,
        key = %key,
        expression = %select_request.expression,
        "Processing S3 Select request"
    );

    // 2. Fetch the object from storage
    let storage = state.storage.read().await;
    let (data, _record) = match storage.get_object(&bucket, &key).await {
        Ok(result) => result,
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("not found") || err_str.contains("does not exist") {
                return S3Error::NoSuchKey.into_response();
            }
            return S3Error::InternalError(err_str).into_response();
        }
    };
    drop(storage); // Release read lock before query execution

    let bytes_scanned = data.len() as u64;

    // 3. Execute the SQL query
    let engine = SelectEngine::new(QueryConfig::default());
    let batches = match engine.execute_single_object(&data, &select_request).await {
        Ok(batches) => batches,
        Err(e) => {
            tracing::warn!(error = %e, "S3 Select query execution failed");
            return select_error_to_response(e);
        }
    };

    // 4. Build the event stream response
    let response_body = match event_stream::build_event_stream_response(
        &batches,
        &select_request.output_serialization,
        bytes_scanned,
    ) {
        Ok(body) => body,
        Err(e) => {
            return S3Error::InternalError(format!("Failed to build response: {e}"))
                .into_response();
        }
    };

    // 5. Return the event stream
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(response_body))
        .unwrap_or_else(|e| {
            S3Error::InternalError(format!("Failed to build HTTP response: {e}")).into_response()
        })
}

/// Handle S4 extended multi-object SQL query.
///
/// **Route**: `POST /{bucket}?sql`
///
/// Accepts a JSON request body with a SQL query and returns results as
/// JSON or CSV with chunked transfer encoding.
pub async fn bucket_sql_query(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    _headers: HeaderMap,
    body: Bytes,
) -> Response {
    // 1. Parse the JSON request body
    let sql_request: MultiObjectSqlRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            return S3Error::InvalidRequest(format!("Invalid JSON request body: {e}"))
                .into_response();
        }
    };

    tracing::debug!(
        bucket = %bucket,
        sql = %sql_request.sql,
        format = %sql_request.format,
        "Processing S4 SQL query"
    );

    // 2. Extract glob pattern from SQL (if any) and rewrite
    let (glob_pattern, rewritten_sql) = match multi_object::extract_glob_pattern(&sql_request.sql) {
        Ok((pattern, sql)) => (pattern, sql),
        Err(e) => {
            return S3Error::InvalidRequest(format!("SQL parsing error: {e}")).into_response();
        }
    };

    // 3. List and fetch matching objects
    let storage = state.storage.read().await;

    // Determine which objects to query
    let keys: Vec<String> = if let Some(ref pattern) = glob_pattern {
        // List all objects and filter by glob
        let prefix = glob_prefix(pattern);
        match storage.list_objects(&bucket, &prefix, 10000).await {
            Ok(objects) => {
                let all_keys: Vec<String> = objects.iter().map(|o| o.0.clone()).collect();
                multi_object::filter_keys_by_glob(&all_keys, pattern)
                    .into_iter()
                    .map(String::from)
                    .collect()
            }
            Err(e) => {
                return S3Error::InternalError(format!("Failed to list objects: {e}"))
                    .into_response();
            }
        }
    } else {
        // No glob pattern — user must specify in the SQL which table to query
        // Default: list all objects in the bucket
        match storage.list_objects(&bucket, "", 10000).await {
            Ok(objects) => objects.iter().map(|o| o.0.clone()).collect(),
            Err(e) => {
                return S3Error::InternalError(format!("Failed to list objects: {e}"))
                    .into_response();
            }
        }
    };

    if keys.is_empty() {
        return S3Error::InvalidRequest("No objects matched the query pattern".to_string())
            .into_response();
    }

    // Fetch all matching objects
    let mut objects: Vec<(&str, &[u8])> = Vec::new();
    let mut fetched_data: Vec<(String, Vec<u8>)> = Vec::new();

    for key in &keys {
        match storage.get_object(&bucket, key).await {
            Ok((data, _record)) => {
                fetched_data.push((key.clone(), data));
            }
            Err(e) => {
                tracing::warn!(key = %key, error = %e, "Failed to fetch object for SQL query");
                // Skip objects that can't be read
            }
        }
    }
    drop(storage); // Release read lock before query execution

    // Build references for the engine
    for (key, data) in &fetched_data {
        objects.push((key.as_str(), data.as_slice()));
    }

    if objects.is_empty() {
        return S3Error::InvalidRequest("No readable objects found".to_string()).into_response();
    }

    // 4. Determine input format
    let input_format = match sql_request.format.to_lowercase().as_str() {
        "csv" => formats::InputFormat::Csv,
        "json" | "jsonl" | "ndjson" => formats::InputFormat::Json,
        "parquet" | "pq" => formats::InputFormat::Parquet,
        other => {
            return S3Error::InvalidRequest(format!("Unsupported format: {other}")).into_response();
        }
    };
    let input_serialization = formats::default_input_serialization(&input_format);

    // 5. Execute the query
    let engine = SelectEngine::new(QueryConfig::default());
    let batches =
        match engine.execute_multi_object(objects, &rewritten_sql, &input_serialization).await {
            Ok(batches) => batches,
            Err(e) => {
                tracing::warn!(error = %e, "S4 SQL query execution failed");
                return select_error_to_response(e);
            }
        };

    // 6. Format output
    let (content_type, output_bytes) = match sql_request.output.to_lowercase().as_str() {
        "csv" => match formats::csv::batches_to_csv_with_headers(&batches) {
            Ok(bytes) => ("text/csv", bytes),
            Err(e) => {
                return S3Error::InternalError(format!("CSV output error: {e}")).into_response();
            }
        },
        _ => {
            // Default: JSON
            match formats::json::batches_to_json_array(&batches) {
                Ok(bytes) => ("application/json", bytes),
                Err(e) => {
                    return S3Error::InternalError(format!("JSON output error: {e}"))
                        .into_response();
                }
            }
        }
    };

    // 7. Return response
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(output_bytes))
        .unwrap_or_else(|e| {
            S3Error::InternalError(format!("Failed to build HTTP response: {e}")).into_response()
        })
}

/// Convert a SelectError to an appropriate S3 error response.
fn select_error_to_response(err: s4_select::SelectError) -> Response {
    use s4_select::SelectError;
    match err {
        SelectError::InvalidExpression(msg) => S3Error::InvalidRequest(msg).into_response(),
        SelectError::UnsupportedFormat(msg) => S3Error::InvalidRequest(msg).into_response(),
        SelectError::InvalidRequest(msg) => S3Error::InvalidRequest(msg).into_response(),
        SelectError::NonSelectStatement(msg) => {
            S3Error::InvalidRequest(format!("Only SELECT statements are allowed: {msg}"))
                .into_response()
        }
        SelectError::ObjectNotFound { bucket, key } => {
            tracing::warn!(bucket = %bucket, key = %key, "Object not found for Select");
            S3Error::NoSuchKey.into_response()
        }
        SelectError::NoMatchingObjects(msg) => S3Error::InvalidRequest(msg).into_response(),
        SelectError::Timeout(secs) => {
            S3Error::InternalError(format!("Query timed out after {secs}s")).into_response()
        }
        SelectError::MemoryLimitExceeded { used, limit } => S3Error::InternalError(format!(
            "Query exceeded memory limit: used {used} bytes, limit {limit} bytes"
        ))
        .into_response(),
        SelectError::ExecutionError(msg) | SelectError::DataParseError(msg) => {
            S3Error::InternalError(msg).into_response()
        }
        SelectError::Internal(msg) => S3Error::InternalError(msg).into_response(),
    }
}

/// Extract a common prefix from a glob pattern for efficient listing.
///
/// E.g., `logs/2024/*.csv` → `logs/2024/`
fn glob_prefix(pattern: &str) -> String {
    if let Some(pos) = pattern.find(['*', '?', '[']) {
        let prefix = &pattern[..pos];
        // Find the last '/' before the glob
        if let Some(slash_pos) = prefix.rfind('/') {
            return prefix[..=slash_pos].to_string();
        }
        return String::new();
    }
    pattern.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_prefix() {
        assert_eq!(glob_prefix("logs/2024/*.csv"), "logs/2024/");
        assert_eq!(glob_prefix("*.csv"), "");
        assert_eq!(glob_prefix("data/file.csv"), "data/file.csv");
        assert_eq!(glob_prefix("logs/*/data/*.json"), "logs/");
    }
}
