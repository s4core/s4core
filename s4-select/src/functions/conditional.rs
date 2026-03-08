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

//! Conditional function implementations: COALESCE, NULLIF.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::compute::kernels::cmp;
use arrow::datatypes::DataType;

use crate::error::SelectError;

/// COALESCE(expr1, expr2, ...) — returns the first non-null argument.
pub fn coalesce(args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef, SelectError> {
    if args.is_empty() {
        return Err(SelectError::InvalidExpression(
            "COALESCE requires at least one argument".to_string(),
        ));
    }

    // Determine result type from first non-null-type arg
    let target_type = args
        .iter()
        .find(|a| *a.data_type() != DataType::Null)
        .map(|a| a.data_type().clone())
        .unwrap_or(DataType::Utf8);

    // Cast all args to the target type
    let casted: Vec<ArrayRef> = args
        .iter()
        .map(|a| {
            if *a.data_type() == DataType::Null {
                arrow::array::new_null_array(&target_type, a.len())
            } else if a.data_type() == &target_type {
                a.clone()
            } else {
                arrow::compute::cast(a, &target_type).unwrap_or_else(|_| a.clone())
            }
        })
        .collect();

    // Build result: for each row, pick the first non-null value
    match &target_type {
        DataType::Utf8 => {
            let result: StringArray = (0..num_rows)
                .map(|i| {
                    for arr in &casted {
                        if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
                            if i < s.len() && s.is_valid(i) {
                                return Some(s.value(i).to_string());
                            }
                        }
                    }
                    None
                })
                .collect();
            Ok(Arc::new(result))
        }
        _ => {
            // For non-string types, return first non-null array
            for arr in &casted {
                if arr.null_count() == 0 {
                    return Ok(arr.clone());
                }
            }
            Ok(casted[0].clone())
        }
    }
}

/// NULLIF(expr1, expr2) — returns null if expr1 = expr2, otherwise expr1.
pub fn nullif(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.len() != 2 {
        return Err(SelectError::InvalidExpression(
            "NULLIF requires exactly 2 arguments".to_string(),
        ));
    }

    let eq_mask = cmp::eq(&args[0], &args[1])
        .map_err(|e| SelectError::ExecutionError(format!("NULLIF comparison failed: {e}")))?;

    let n = args[0].len();
    let dt = args[0].data_type();

    match dt {
        DataType::Utf8 => {
            let s = args[0].as_any().downcast_ref::<StringArray>().unwrap();
            let result: StringArray = (0..n)
                .map(|i| {
                    if eq_mask.is_valid(i) && eq_mask.value(i) {
                        None
                    } else if s.is_valid(i) {
                        Some(s.value(i).to_string())
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Int64 => {
            let a = args[0].as_any().downcast_ref::<Int64Array>().unwrap();
            let result: Int64Array = (0..n)
                .map(|i| {
                    if eq_mask.is_valid(i) && eq_mask.value(i) {
                        None
                    } else if a.is_valid(i) {
                        Some(a.value(i))
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Float64 => {
            let a = args[0].as_any().downcast_ref::<Float64Array>().unwrap();
            let result: Float64Array = (0..n)
                .map(|i| {
                    if eq_mask.is_valid(i) && eq_mask.value(i) {
                        None
                    } else if a.is_valid(i) {
                        Some(a.value(i))
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }
        _ => {
            let casted = arrow::compute::cast(&args[0], &DataType::Utf8)
                .map_err(|e| SelectError::ExecutionError(format!("NULLIF cast: {e}")))?;
            let s = casted.as_any().downcast_ref::<StringArray>().unwrap();
            let result: StringArray = (0..n)
                .map(|i| {
                    if eq_mask.is_valid(i) && eq_mask.value(i) {
                        None
                    } else if s.is_valid(i) {
                        Some(s.value(i).to_string())
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }
    }
}
