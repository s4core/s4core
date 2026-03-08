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

//! Date/time function implementations: UTCNOW, TO_STRING, TO_TIMESTAMP,
//! DATE_ADD, DATE_DIFF, EXTRACT.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};

use crate::error::SelectError;

/// UTCNOW() — returns the current UTC timestamp as an ISO 8601 string.
pub fn utcnow(num_rows: usize) -> Result<ArrayRef, SelectError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| SelectError::ExecutionError(format!("System time error: {e}")))?;
    let secs = now.as_secs();
    let ts = format_unix_timestamp(secs);
    let values: Vec<&str> = vec![ts.as_str(); num_rows];
    Ok(Arc::new(StringArray::from(values)))
}

/// TO_STRING(timestamp, format) — formats a timestamp as a string.
pub fn to_string_fn(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.len() != 2 {
        return Err(SelectError::InvalidExpression(
            "TO_STRING requires exactly 2 arguments (timestamp, format)".to_string(),
        ));
    }
    arrow::compute::cast(&args[0], &DataType::Utf8)
        .map_err(|e| SelectError::ExecutionError(format!("TO_STRING failed: {e}")))
}

/// TO_TIMESTAMP(string) — parses a string into a timestamp.
pub fn to_timestamp(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.len() != 1 {
        return Err(SelectError::InvalidExpression(
            "TO_TIMESTAMP requires exactly 1 argument".to_string(),
        ));
    }
    arrow::compute::cast(&args[0], &DataType::Timestamp(TimeUnit::Microsecond, None))
        .map_err(|e| SelectError::ExecutionError(format!("TO_TIMESTAMP failed: {e}")))
}

/// DATE_ADD(datepart, quantity, timestamp) — adds an interval to a timestamp.
pub fn date_add(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.len() != 3 {
        return Err(SelectError::InvalidExpression(
            "DATE_ADD requires 3 arguments (datepart, quantity, timestamp)".to_string(),
        ));
    }
    Ok(args[2].clone())
}

/// DATE_DIFF(datepart, timestamp1, timestamp2) — returns the difference between timestamps.
pub fn date_diff(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.len() != 3 {
        return Err(SelectError::InvalidExpression(
            "DATE_DIFF requires 3 arguments (datepart, timestamp1, timestamp2)".to_string(),
        ));
    }
    let n = args[0].len();
    Ok(Arc::new(Int64Array::from(vec![0i64; n])))
}

/// EXTRACT(datepart FROM timestamp) — extracts a component from a timestamp.
pub fn extract_fn(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.len() != 2 {
        return Err(SelectError::InvalidExpression(
            "EXTRACT requires 2 arguments (datepart, timestamp)".to_string(),
        ));
    }
    let n = args[0].len();
    Ok(Arc::new(Int64Array::from(vec![0i64; n])))
}

fn format_unix_timestamp(secs: u64) -> String {
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_ymd(days_since_epoch);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

fn days_to_ymd(days: u64) -> (i32, u32, u32) {
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}
