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

//! Aggregate function implementations: COUNT, SUM, AVG, MIN, MAX.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;

use crate::error::SelectError;
use crate::eval::evaluate_expr;
use crate::planner::{AggregateFunc, ResolvedExpr};

/// Compute an aggregate function across all batches.
pub fn compute_aggregate(
    func: &AggregateFunc,
    arg: &Option<Box<ResolvedExpr>>,
    batches: &[RecordBatch],
) -> Result<(ArrayRef, DataType), SelectError> {
    match func {
        AggregateFunc::CountStar => {
            let count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
            Ok((Arc::new(Int64Array::from(vec![count])), DataType::Int64))
        }
        AggregateFunc::Count => {
            let arg = arg.as_ref().ok_or_else(|| {
                SelectError::ExecutionError("COUNT requires an argument".to_string())
            })?;
            let mut count: i64 = 0;
            for batch in batches {
                let arr = evaluate_expr(arg, batch)?;
                count += (arr.len() - arr.null_count()) as i64;
            }
            Ok((Arc::new(Int64Array::from(vec![count])), DataType::Int64))
        }
        AggregateFunc::Sum => compute_sum(arg, batches),
        AggregateFunc::Avg => compute_avg(arg, batches),
        AggregateFunc::Min => compute_min(arg, batches),
        AggregateFunc::Max => compute_max(arg, batches),
    }
}

fn compute_sum(
    arg: &Option<Box<ResolvedExpr>>,
    batches: &[RecordBatch],
) -> Result<(ArrayRef, DataType), SelectError> {
    let arg = arg
        .as_ref()
        .ok_or_else(|| SelectError::ExecutionError("SUM requires an argument".to_string()))?;

    let mut sum_f64: f64 = 0.0;
    let mut is_float = false;
    let mut sum_i64: i64 = 0;

    for batch in batches {
        let arr = evaluate_expr(arg, batch)?;
        match arr.data_type() {
            DataType::Int64 => {
                let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        sum_i64 += a.value(i);
                        sum_f64 += a.value(i) as f64;
                    }
                }
            }
            DataType::Float64 => {
                is_float = true;
                let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        sum_f64 += a.value(i);
                    }
                }
            }
            dt => {
                is_float = true;
                let casted = arrow::compute::cast(&arr, &DataType::Float64).map_err(|e| {
                    SelectError::ExecutionError(format!("SUM: cannot sum type {dt:?}: {e}"))
                })?;
                let a = casted.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        sum_f64 += a.value(i);
                    }
                }
            }
        }
    }

    if is_float {
        Ok((
            Arc::new(Float64Array::from(vec![sum_f64])),
            DataType::Float64,
        ))
    } else {
        Ok((Arc::new(Int64Array::from(vec![sum_i64])), DataType::Int64))
    }
}

fn compute_avg(
    arg: &Option<Box<ResolvedExpr>>,
    batches: &[RecordBatch],
) -> Result<(ArrayRef, DataType), SelectError> {
    let arg = arg
        .as_ref()
        .ok_or_else(|| SelectError::ExecutionError("AVG requires an argument".to_string()))?;

    let mut sum: f64 = 0.0;
    let mut count: i64 = 0;

    for batch in batches {
        let arr = evaluate_expr(arg, batch)?;
        let casted = arrow::compute::cast(&arr, &DataType::Float64).map_err(|e| {
            SelectError::ExecutionError(format!("AVG: cannot convert to float: {e}"))
        })?;
        let a = casted.as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..a.len() {
            if a.is_valid(i) {
                sum += a.value(i);
                count += 1;
            }
        }
    }

    let avg = if count > 0 { sum / count as f64 } else { 0.0 };
    Ok((Arc::new(Float64Array::from(vec![avg])), DataType::Float64))
}

fn compute_min(
    arg: &Option<Box<ResolvedExpr>>,
    batches: &[RecordBatch],
) -> Result<(ArrayRef, DataType), SelectError> {
    let arg = arg
        .as_ref()
        .ok_or_else(|| SelectError::ExecutionError("MIN requires an argument".to_string()))?;

    let mut min_i64: Option<i64> = None;
    let mut min_f64: Option<f64> = None;
    let mut min_str: Option<String> = None;
    let mut data_type = DataType::Null;

    for batch in batches {
        let arr = evaluate_expr(arg, batch)?;
        data_type = arr.data_type().clone();
        match arr.data_type() {
            DataType::Int64 => {
                let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        let v = a.value(i);
                        min_i64 = Some(min_i64.map_or(v, |m: i64| m.min(v)));
                    }
                }
            }
            DataType::Float64 => {
                let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        let v = a.value(i);
                        min_f64 = Some(min_f64.map_or(v, |m: f64| m.min(v)));
                    }
                }
            }
            DataType::Utf8 => {
                let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        let v = a.value(i).to_string();
                        min_str =
                            Some(min_str.map_or(v.clone(), |m: String| if v < m { v } else { m }));
                    }
                }
            }
            _ => {
                return Err(SelectError::ExecutionError(format!(
                    "MIN not supported for type {:?}",
                    arr.data_type()
                )));
            }
        }
    }

    if let Some(v) = min_i64 {
        Ok((Arc::new(Int64Array::from(vec![v])), DataType::Int64))
    } else if let Some(v) = min_f64 {
        Ok((Arc::new(Float64Array::from(vec![v])), DataType::Float64))
    } else if let Some(v) = min_str {
        Ok((
            Arc::new(StringArray::from(vec![v.as_str()])),
            DataType::Utf8,
        ))
    } else {
        Ok((arrow::array::new_null_array(&data_type, 1), data_type))
    }
}

fn compute_max(
    arg: &Option<Box<ResolvedExpr>>,
    batches: &[RecordBatch],
) -> Result<(ArrayRef, DataType), SelectError> {
    let arg = arg
        .as_ref()
        .ok_or_else(|| SelectError::ExecutionError("MAX requires an argument".to_string()))?;

    let mut max_i64: Option<i64> = None;
    let mut max_f64: Option<f64> = None;
    let mut max_str: Option<String> = None;
    let mut data_type = DataType::Null;

    for batch in batches {
        let arr = evaluate_expr(arg, batch)?;
        data_type = arr.data_type().clone();
        match arr.data_type() {
            DataType::Int64 => {
                let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        let v = a.value(i);
                        max_i64 = Some(max_i64.map_or(v, |m: i64| m.max(v)));
                    }
                }
            }
            DataType::Float64 => {
                let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        let v = a.value(i);
                        max_f64 = Some(max_f64.map_or(v, |m: f64| m.max(v)));
                    }
                }
            }
            DataType::Utf8 => {
                let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        let v = a.value(i).to_string();
                        max_str =
                            Some(max_str.map_or(v.clone(), |m: String| if v > m { v } else { m }));
                    }
                }
            }
            _ => {
                return Err(SelectError::ExecutionError(format!(
                    "MAX not supported for type {:?}",
                    arr.data_type()
                )));
            }
        }
    }

    if let Some(v) = max_i64 {
        Ok((Arc::new(Int64Array::from(vec![v])), DataType::Int64))
    } else if let Some(v) = max_f64 {
        Ok((Arc::new(Float64Array::from(vec![v])), DataType::Float64))
    } else if let Some(v) = max_str {
        Ok((
            Arc::new(StringArray::from(vec![v.as_str()])),
            DataType::Utf8,
        ))
    } else {
        Ok((arrow::array::new_null_array(&data_type, 1), data_type))
    }
}
