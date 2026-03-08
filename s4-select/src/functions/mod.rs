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

//! Built-in function implementations for S3 Select SQL.

pub mod aggregate;
pub mod conditional;
pub mod conversion;
pub mod date;
pub mod string;

use arrow::array::ArrayRef;

use crate::error::SelectError;

/// Dispatch a scalar function call by name.
pub fn call_scalar_function(
    name: &str,
    args: &[ArrayRef],
    num_rows: usize,
) -> Result<ArrayRef, SelectError> {
    match name {
        "CHAR_LENGTH" | "CHARACTER_LENGTH" | "LENGTH" => string::char_length(args),
        "LOWER" => string::lower(args),
        "UPPER" => string::upper(args),
        "SUBSTRING" | "SUBSTR" => string::substring(args),
        "TRIM" => string::trim(args),
        "COALESCE" => conditional::coalesce(args, num_rows),
        "NULLIF" => conditional::nullif(args),
        "CAST" => conversion::cast_fn(args),
        "UTCNOW" => date::utcnow(num_rows),
        "TO_STRING" => date::to_string_fn(args),
        "TO_TIMESTAMP" => date::to_timestamp(args),
        "DATE_ADD" => date::date_add(args),
        "DATE_DIFF" => date::date_diff(args),
        "EXTRACT" => date::extract_fn(args),
        _ => Err(SelectError::InvalidExpression(format!(
            "Unknown function: {name}"
        ))),
    }
}
