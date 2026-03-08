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

//! String function implementations: CHAR_LENGTH, LOWER, UPPER, SUBSTRING, TRIM.

use std::sync::Arc;

use arrow::array::{cast::AsArray, Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::DataType;

use crate::error::SelectError;

/// CHAR_LENGTH / CHARACTER_LENGTH / LENGTH
pub fn char_length(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.len() != 1 {
        return Err(SelectError::InvalidExpression(
            "CHAR_LENGTH requires exactly one argument".to_string(),
        ));
    }
    let arr = cast_to_string(&args[0])?;
    let s = arr.as_string::<i32>();
    let result: Int64Array = (0..s.len())
        .map(|i| {
            if s.is_valid(i) {
                Some(s.value(i).chars().count() as i64)
            } else {
                None
            }
        })
        .collect();
    Ok(Arc::new(result))
}

/// LOWER
pub fn lower(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.len() != 1 {
        return Err(SelectError::InvalidExpression(
            "LOWER requires exactly one argument".to_string(),
        ));
    }
    let arr = cast_to_string(&args[0])?;
    let s = arr.as_string::<i32>();
    let result: StringArray = (0..s.len())
        .map(|i| {
            if s.is_valid(i) {
                Some(s.value(i).to_lowercase())
            } else {
                None
            }
        })
        .collect();
    Ok(Arc::new(result))
}

/// UPPER
pub fn upper(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.len() != 1 {
        return Err(SelectError::InvalidExpression(
            "UPPER requires exactly one argument".to_string(),
        ));
    }
    let arr = cast_to_string(&args[0])?;
    let s = arr.as_string::<i32>();
    let result: StringArray = (0..s.len())
        .map(|i| {
            if s.is_valid(i) {
                Some(s.value(i).to_uppercase())
            } else {
                None
            }
        })
        .collect();
    Ok(Arc::new(result))
}

/// SUBSTRING(string, start [, length])
pub fn substring(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(SelectError::InvalidExpression(
            "SUBSTRING requires 2 or 3 arguments".to_string(),
        ));
    }

    let str_arr = cast_to_string(&args[0])?;
    let s = str_arr.as_string::<i32>();

    let start_arr = arrow::compute::cast(&args[1], &DataType::Int64)
        .map_err(|e| SelectError::ExecutionError(format!("SUBSTRING start: {e}")))?;
    let starts = start_arr.as_any().downcast_ref::<Int64Array>().unwrap();

    let len_arr = if args.len() == 3 {
        Some(
            arrow::compute::cast(&args[2], &DataType::Int64)
                .map_err(|e| SelectError::ExecutionError(format!("SUBSTRING length: {e}")))?,
        )
    } else {
        None
    };
    let lengths = len_arr.as_ref().map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap());

    let result: StringArray = (0..s.len())
        .map(|i| {
            if !s.is_valid(i) || !starts.is_valid(i) {
                return None;
            }
            let val = s.value(i);
            let start = starts.value(i);
            let start_idx = (start - 1).max(0) as usize;
            let chars: Vec<char> = val.chars().collect();

            if start_idx >= chars.len() {
                return Some(String::new());
            }

            if let Some(lens) = lengths {
                if lens.is_valid(i) {
                    let len = lens.value(i).max(0) as usize;
                    let end = (start_idx + len).min(chars.len());
                    Some(chars[start_idx..end].iter().collect())
                } else {
                    None
                }
            } else {
                Some(chars[start_idx..].iter().collect())
            }
        })
        .collect();

    Ok(Arc::new(result))
}

/// TRIM([LEADING|TRAILING|BOTH] [char FROM] string)
pub fn trim(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.is_empty() {
        return Err(SelectError::InvalidExpression(
            "TRIM requires at least one argument".to_string(),
        ));
    }

    let str_arr = cast_to_string(&args[0])?;
    let s = str_arr.as_string::<i32>();

    let where_part = if args.len() > 1 {
        let w = cast_to_string(&args[1])?;
        let ws = w.as_string::<i32>();
        if ws.is_valid(0) {
            ws.value(0).to_string()
        } else {
            "BOTH".to_string()
        }
    } else {
        "BOTH".to_string()
    };

    let trim_char = if args.len() > 2 {
        let c = cast_to_string(&args[2])?;
        let cs = c.as_string::<i32>();
        if cs.is_valid(0) {
            cs.value(0).chars().next()
        } else {
            None
        }
    } else {
        None
    };

    let result: StringArray = (0..s.len())
        .map(|i| {
            if !s.is_valid(i) {
                return None;
            }
            let val = s.value(i);
            Some(if let Some(ch) = trim_char {
                match where_part.as_str() {
                    "LEADING" => val.trim_start_matches(ch).to_string(),
                    "TRAILING" => val.trim_end_matches(ch).to_string(),
                    _ => val.trim_matches(ch).to_string(),
                }
            } else {
                match where_part.as_str() {
                    "LEADING" => val.trim_start().to_string(),
                    "TRAILING" => val.trim_end().to_string(),
                    _ => val.trim().to_string(),
                }
            })
        })
        .collect();

    Ok(Arc::new(result))
}

fn cast_to_string(arr: &ArrayRef) -> Result<ArrayRef, SelectError> {
    if arr.data_type() == &DataType::Utf8 {
        return Ok(arr.clone());
    }
    arrow::compute::cast(arr, &DataType::Utf8)
        .map_err(|e| SelectError::ExecutionError(format!("Failed to cast to string: {e}")))
}
