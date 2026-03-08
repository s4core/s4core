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

//! Expression evaluator — evaluates [`ResolvedExpr`] against Arrow RecordBatches.
//!
//! Uses Arrow compute kernels for vectorized operations on columnar data.

use std::sync::Arc;

use arrow::array::{
    cast::AsArray, new_null_array, Array, ArrayRef, BooleanArray, Float64Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cmp;
use arrow::datatypes::{DataType, Field, Schema};

use crate::error::SelectError;
use crate::functions;
use crate::planner::{BinOp, ResolvedExpr, ScalarValue, SelectPlan, UnOp};

/// Evaluate an expression against a RecordBatch, producing an Arrow array.
pub fn evaluate_expr(expr: &ResolvedExpr, batch: &RecordBatch) -> Result<ArrayRef, SelectError> {
    match expr {
        ResolvedExpr::ColumnIndex(idx) => {
            if *idx >= batch.num_columns() {
                return Err(SelectError::ExecutionError(format!(
                    "Column index {} out of range (batch has {} columns)",
                    idx,
                    batch.num_columns()
                )));
            }
            Ok(batch.column(*idx).clone())
        }

        ResolvedExpr::Literal(scalar) => scalar_to_array(scalar, batch.num_rows()),

        ResolvedExpr::BinaryOp { left, op, right } => {
            let left_arr = evaluate_expr(left, batch)?;
            let right_arr = evaluate_expr(right, batch)?;
            eval_binary_op(&left_arr, *op, &right_arr)
        }

        ResolvedExpr::UnaryOp { op, expr } => {
            let arr = evaluate_expr(expr, batch)?;
            eval_unary_op(*op, &arr)
        }

        ResolvedExpr::IsNull(inner) => {
            let arr = evaluate_expr(inner, batch)?;
            let result = arrow::compute::is_null(&arr)?;
            Ok(Arc::new(result))
        }

        ResolvedExpr::IsNotNull(inner) => {
            let arr = evaluate_expr(inner, batch)?;
            let result = arrow::compute::is_not_null(&arr)?;
            Ok(Arc::new(result))
        }

        ResolvedExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let val = evaluate_expr(expr, batch)?;
            let low_val = evaluate_expr(low, batch)?;
            let high_val = evaluate_expr(high, batch)?;
            let ge_low = cmp::gt_eq(&val, &low_val)?;
            let le_high = cmp::lt_eq(&val, &high_val)?;
            let result = arrow::compute::and(&ge_low, &le_high)?;
            if *negated {
                Ok(Arc::new(arrow::compute::not(&result)?))
            } else {
                Ok(Arc::new(result))
            }
        }

        ResolvedExpr::InList {
            expr,
            list,
            negated,
        } => {
            let val = evaluate_expr(expr, batch)?;
            let mut result: Option<BooleanArray> = None;
            for item in list {
                let item_arr = evaluate_expr(item, batch)?;
                let eq = cmp::eq(&val, &item_arr)?;
                result = Some(match result {
                    Some(prev) => arrow::compute::or(&prev, &eq)?,
                    None => eq,
                });
            }
            let result =
                result.unwrap_or_else(|| BooleanArray::from(vec![false; batch.num_rows()]));
            if *negated {
                Ok(Arc::new(arrow::compute::not(&result)?))
            } else {
                Ok(Arc::new(result))
            }
        }

        ResolvedExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let expr_arr = evaluate_expr(expr, batch)?;
            let pattern_arr = evaluate_expr(pattern, batch)?;
            eval_like(&expr_arr, &pattern_arr, *negated)
        }

        ResolvedExpr::Case {
            operand,
            when_then,
            else_expr,
        } => eval_case(operand, when_then, else_expr, batch),

        ResolvedExpr::Cast { expr, data_type } => {
            let arr = evaluate_expr(expr, batch)?;
            let result = arrow::compute::cast(&arr, data_type)
                .map_err(|e| SelectError::ExecutionError(format!("CAST failed: {e}")))?;
            Ok(result)
        }

        ResolvedExpr::Nested(inner) => evaluate_expr(inner, batch),

        ResolvedExpr::Function { name, args } => {
            let evaluated_args: Vec<ArrayRef> =
                args.iter().map(|a| evaluate_expr(a, batch)).collect::<Result<Vec<_>, _>>()?;
            functions::call_scalar_function(name, &evaluated_args, batch.num_rows())
        }

        ResolvedExpr::Aggregate { .. } => Err(SelectError::ExecutionError(
            "Aggregate functions cannot be evaluated per-batch; use execute_aggregate".to_string(),
        )),
    }
}

/// Convert a scalar value to an Arrow array of the given length.
fn scalar_to_array(value: &ScalarValue, len: usize) -> Result<ArrayRef, SelectError> {
    match value {
        ScalarValue::Int64(v) => Ok(Arc::new(Int64Array::from(vec![*v; len]))),
        ScalarValue::Float64(v) => Ok(Arc::new(Float64Array::from(vec![*v; len]))),
        ScalarValue::Utf8(v) => Ok(Arc::new(StringArray::from(vec![v.as_str(); len]))),
        ScalarValue::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![*v; len]))),
        ScalarValue::Null => Ok(new_null_array(&DataType::Null, len)),
    }
}

/// Evaluate a binary operation.
fn eval_binary_op(left: &ArrayRef, op: BinOp, right: &ArrayRef) -> Result<ArrayRef, SelectError> {
    // Cast both sides to compatible types if needed
    let (left, right) = coerce_types(left, right)?;

    match op {
        // Arithmetic
        BinOp::Add => Ok(arrow::compute::kernels::numeric::add(&left, &right)?),
        BinOp::Sub => Ok(arrow::compute::kernels::numeric::sub(&left, &right)?),
        BinOp::Mul => Ok(arrow::compute::kernels::numeric::mul(&left, &right)?),
        BinOp::Div => Ok(arrow::compute::kernels::numeric::div(&left, &right)?),
        BinOp::Mod => Ok(arrow::compute::kernels::numeric::rem(&left, &right)?),
        // Comparison
        BinOp::Eq => Ok(Arc::new(cmp::eq(&left, &right)?)),
        BinOp::Neq => Ok(Arc::new(cmp::neq(&left, &right)?)),
        BinOp::Lt => Ok(Arc::new(cmp::lt(&left, &right)?)),
        BinOp::LtEq => Ok(Arc::new(cmp::lt_eq(&left, &right)?)),
        BinOp::Gt => Ok(Arc::new(cmp::gt(&left, &right)?)),
        BinOp::GtEq => Ok(Arc::new(cmp::gt_eq(&left, &right)?)),
        // Logical
        BinOp::And => {
            let left_bool = as_boolean_array(&left)?;
            let right_bool = as_boolean_array(&right)?;
            Ok(Arc::new(arrow::compute::and(left_bool, right_bool)?))
        }
        BinOp::Or => {
            let left_bool = as_boolean_array(&left)?;
            let right_bool = as_boolean_array(&right)?;
            Ok(Arc::new(arrow::compute::or(left_bool, right_bool)?))
        }
        // String concat
        BinOp::Concat => {
            let left_str = cast_to_string(&left)?;
            let right_str = cast_to_string(&right)?;
            let left_s = left_str.as_string::<i32>();
            let right_s = right_str.as_string::<i32>();
            let result: StringArray = left_s
                .iter()
                .zip(right_s.iter())
                .map(|(l, r)| match (l, r) {
                    (Some(l), Some(r)) => Some(format!("{l}{r}")),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }
    }
}

/// Evaluate a unary operation.
fn eval_unary_op(op: UnOp, arr: &ArrayRef) -> Result<ArrayRef, SelectError> {
    match op {
        UnOp::Not => {
            let bool_arr = as_boolean_array(arr)?;
            Ok(Arc::new(arrow::compute::not(bool_arr)?))
        }
        UnOp::Minus => {
            let negated = arrow::compute::kernels::numeric::neg(arr)
                .map_err(|e| SelectError::ExecutionError(format!("Negation failed: {e}")))?;
            Ok(negated)
        }
    }
}

/// Coerce two arrays to compatible types for binary operations.
fn coerce_types(left: &ArrayRef, right: &ArrayRef) -> Result<(ArrayRef, ArrayRef), SelectError> {
    let lt = left.data_type();
    let rt = right.data_type();

    if lt == rt {
        return Ok((left.clone(), right.clone()));
    }

    // Handle Null type
    if *lt == DataType::Null {
        let casted = arrow::compute::cast(left, rt)
            .map_err(|e| SelectError::ExecutionError(format!("Type coercion failed: {e}")))?;
        return Ok((casted, right.clone()));
    }
    if *rt == DataType::Null {
        let casted = arrow::compute::cast(right, lt)
            .map_err(|e| SelectError::ExecutionError(format!("Type coercion failed: {e}")))?;
        return Ok((left.clone(), casted));
    }

    // Numeric promotions: Int64 <-> Float64
    if is_numeric(lt) && is_numeric(rt) {
        let target = if *lt == DataType::Float64 || *rt == DataType::Float64 {
            DataType::Float64
        } else {
            DataType::Int64
        };
        let l = arrow::compute::cast(left, &target)
            .map_err(|e| SelectError::ExecutionError(format!("Type coercion failed: {e}")))?;
        let r = arrow::compute::cast(right, &target)
            .map_err(|e| SelectError::ExecutionError(format!("Type coercion failed: {e}")))?;
        return Ok((l, r));
    }

    // String comparison with numeric — cast numeric to string
    if is_string(lt) && is_numeric(rt) {
        let r = arrow::compute::cast(right, &DataType::Utf8)
            .map_err(|e| SelectError::ExecutionError(format!("Type coercion failed: {e}")))?;
        return Ok((left.clone(), r));
    }
    if is_numeric(lt) && is_string(rt) {
        let l = arrow::compute::cast(left, &DataType::Utf8)
            .map_err(|e| SelectError::ExecutionError(format!("Type coercion failed: {e}")))?;
        return Ok((l, right.clone()));
    }

    // Try casting right to left's type
    if let Ok(r) = arrow::compute::cast(right, lt) {
        return Ok((left.clone(), r));
    }
    // Try casting left to right's type
    if let Ok(l) = arrow::compute::cast(left, rt) {
        return Ok((l, right.clone()));
    }

    Err(SelectError::ExecutionError(format!(
        "Cannot coerce types: {lt:?} and {rt:?}"
    )))
}

fn is_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

fn is_string(dt: &DataType) -> bool {
    matches!(dt, DataType::Utf8 | DataType::LargeUtf8)
}

fn as_boolean_array(arr: &ArrayRef) -> Result<&BooleanArray, SelectError> {
    arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
        SelectError::ExecutionError(format!("Expected boolean array, got {:?}", arr.data_type()))
    })
}

fn cast_to_string(arr: &ArrayRef) -> Result<ArrayRef, SelectError> {
    if arr.data_type() == &DataType::Utf8 {
        return Ok(arr.clone());
    }
    arrow::compute::cast(arr, &DataType::Utf8)
        .map_err(|e| SelectError::ExecutionError(format!("Failed to cast to string: {e}")))
}

/// Evaluate LIKE expression.
fn eval_like(expr: &ArrayRef, pattern: &ArrayRef, negated: bool) -> Result<ArrayRef, SelectError> {
    let expr_str = arrow::compute::cast(expr, &DataType::Utf8)
        .map_err(|e| SelectError::ExecutionError(format!("LIKE cast failed: {e}")))?;
    let pattern_str = arrow::compute::cast(pattern, &DataType::Utf8)
        .map_err(|e| SelectError::ExecutionError(format!("LIKE cast failed: {e}")))?;

    let expr_s = expr_str.as_string::<i32>();
    let pattern_s = pattern_str.as_string::<i32>();

    let result = if negated {
        arrow::compute::nlike(expr_s, pattern_s)
            .map_err(|e| SelectError::ExecutionError(format!("LIKE failed: {e}")))?
    } else {
        arrow::compute::like(expr_s, pattern_s)
            .map_err(|e| SelectError::ExecutionError(format!("LIKE failed: {e}")))?
    };

    Ok(Arc::new(result))
}

/// Evaluate CASE expression.
fn eval_case(
    operand: &Option<Box<ResolvedExpr>>,
    when_then: &[(ResolvedExpr, ResolvedExpr)],
    else_expr: &Option<Box<ResolvedExpr>>,
    batch: &RecordBatch,
) -> Result<ArrayRef, SelectError> {
    let n = batch.num_rows();

    // Track which rows have been matched
    let mut matched = vec![false; n];
    // Collect results (start with null)
    let mut result_arrays: Vec<(BooleanArray, ArrayRef)> = Vec::new();

    let operand_val = operand.as_ref().map(|e| evaluate_expr(e, batch)).transpose()?;

    for (when_expr, then_expr) in when_then {
        let condition = if let Some(ref op_val) = operand_val {
            // Simple CASE: compare operand = when_expr
            let when_val = evaluate_expr(when_expr, batch)?;
            Arc::new(cmp::eq(op_val, &when_val)?) as ArrayRef
        } else {
            // Searched CASE: when_expr is the condition
            evaluate_expr(when_expr, batch)?
        };

        let cond_bool = as_boolean_array(&condition)?;
        let then_val = evaluate_expr(then_expr, batch)?;

        // Build mask: only rows not yet matched AND condition is true
        let mask: BooleanArray = cond_bool
            .iter()
            .enumerate()
            .map(|(i, v)| {
                if matched[i] {
                    Some(false)
                } else if v == Some(true) {
                    matched[i] = true;
                    Some(true)
                } else {
                    Some(false)
                }
            })
            .collect();

        result_arrays.push((mask, then_val));
    }

    // Determine result data type from first THEN value
    let result_type = if let Some((_, arr)) = result_arrays.first() {
        arr.data_type().clone()
    } else if let Some(ref e) = else_expr {
        let else_val = evaluate_expr(e, batch)?;
        return Ok(else_val);
    } else {
        return Ok(new_null_array(&DataType::Null, n));
    };

    // Build result array row by row
    let else_val = else_expr.as_ref().map(|e| evaluate_expr(e, batch)).transpose()?;

    // Use a simple approach: iterate rows and pick the right value
    match &result_type {
        DataType::Utf8 => build_case_result_string(&result_arrays, &else_val, n, &matched),
        DataType::Int64 => build_case_result_int64(&result_arrays, &else_val, n, &matched),
        DataType::Float64 => build_case_result_float64(&result_arrays, &else_val, n, &matched),
        DataType::Boolean => build_case_result_bool(&result_arrays, &else_val, n, &matched),
        _ => {
            // Fallback: cast everything to string
            build_case_result_string(&result_arrays, &else_val, n, &matched)
        }
    }
}

#[allow(clippy::needless_range_loop)]
fn build_case_result_string(
    branches: &[(BooleanArray, ArrayRef)],
    else_val: &Option<ArrayRef>,
    n: usize,
    _matched: &[bool],
) -> Result<ArrayRef, SelectError> {
    let mut result: Vec<Option<String>> = vec![None; n];
    for (mask, arr) in branches {
        let str_arr = arrow::compute::cast(arr, &DataType::Utf8)
            .map_err(|e| SelectError::ExecutionError(format!("CASE cast: {e}")))?;
        let s = str_arr.as_string::<i32>();
        for i in 0..n {
            if result[i].is_none() && mask.value(i) {
                result[i] = s.is_valid(i).then(|| s.value(i).to_string());
            }
        }
    }
    if let Some(ref e) = else_val {
        let str_arr = arrow::compute::cast(e, &DataType::Utf8)
            .map_err(|er| SelectError::ExecutionError(format!("CASE cast: {er}")))?;
        let s = str_arr.as_string::<i32>();
        for i in 0..n {
            if result[i].is_none() && s.is_valid(i) {
                result[i] = Some(s.value(i).to_string());
            }
        }
    }
    Ok(Arc::new(StringArray::from(result)))
}

#[allow(clippy::needless_range_loop)]
fn build_case_result_int64(
    branches: &[(BooleanArray, ArrayRef)],
    else_val: &Option<ArrayRef>,
    n: usize,
    _matched: &[bool],
) -> Result<ArrayRef, SelectError> {
    let mut result: Vec<Option<i64>> = vec![None; n];
    for (mask, arr) in branches {
        let casted = arrow::compute::cast(arr, &DataType::Int64)
            .map_err(|e| SelectError::ExecutionError(format!("CASE cast: {e}")))?;
        let i_arr = casted.as_primitive::<arrow::datatypes::Int64Type>();
        for i in 0..n {
            if result[i].is_none() && mask.value(i) && i_arr.is_valid(i) {
                result[i] = Some(i_arr.value(i));
            }
        }
    }
    if let Some(ref e) = else_val {
        let casted = arrow::compute::cast(e, &DataType::Int64)
            .map_err(|er| SelectError::ExecutionError(format!("CASE cast: {er}")))?;
        let i_arr = casted.as_primitive::<arrow::datatypes::Int64Type>();
        for i in 0..n {
            if result[i].is_none() && i_arr.is_valid(i) {
                result[i] = Some(i_arr.value(i));
            }
        }
    }
    Ok(Arc::new(Int64Array::from(result)))
}

#[allow(clippy::needless_range_loop)]
fn build_case_result_float64(
    branches: &[(BooleanArray, ArrayRef)],
    else_val: &Option<ArrayRef>,
    n: usize,
    _matched: &[bool],
) -> Result<ArrayRef, SelectError> {
    let mut result: Vec<Option<f64>> = vec![None; n];
    for (mask, arr) in branches {
        let casted = arrow::compute::cast(arr, &DataType::Float64)
            .map_err(|e| SelectError::ExecutionError(format!("CASE cast: {e}")))?;
        let f_arr = casted.as_primitive::<arrow::datatypes::Float64Type>();
        for i in 0..n {
            if result[i].is_none() && mask.value(i) && f_arr.is_valid(i) {
                result[i] = Some(f_arr.value(i));
            }
        }
    }
    if let Some(ref e) = else_val {
        let casted = arrow::compute::cast(e, &DataType::Float64)
            .map_err(|er| SelectError::ExecutionError(format!("CASE cast: {er}")))?;
        let f_arr = casted.as_primitive::<arrow::datatypes::Float64Type>();
        for i in 0..n {
            if result[i].is_none() && f_arr.is_valid(i) {
                result[i] = Some(f_arr.value(i));
            }
        }
    }
    Ok(Arc::new(Float64Array::from(result)))
}

#[allow(clippy::needless_range_loop)]
fn build_case_result_bool(
    branches: &[(BooleanArray, ArrayRef)],
    else_val: &Option<ArrayRef>,
    n: usize,
    _matched: &[bool],
) -> Result<ArrayRef, SelectError> {
    let mut result: Vec<Option<bool>> = vec![None; n];
    for (mask, arr) in branches {
        let b_arr = arr.as_any().downcast_ref::<BooleanArray>();
        if let Some(b_arr) = b_arr {
            for i in 0..n {
                if result[i].is_none() && mask.value(i) && b_arr.is_valid(i) {
                    result[i] = Some(b_arr.value(i));
                }
            }
        }
    }
    if let Some(ref e) = else_val {
        if let Some(b_arr) = e.as_any().downcast_ref::<BooleanArray>() {
            for i in 0..n {
                if result[i].is_none() && b_arr.is_valid(i) {
                    result[i] = Some(b_arr.value(i));
                }
            }
        }
    }
    Ok(Arc::new(BooleanArray::from(result)))
}

/// Filter a RecordBatch using a WHERE expression.
pub fn filter_batch(
    batch: &RecordBatch,
    filter_expr: &ResolvedExpr,
) -> Result<RecordBatch, SelectError> {
    let mask = evaluate_expr(filter_expr, batch)?;
    let bool_mask = as_boolean_array(&mask)?;
    Ok(filter_record_batch(batch, bool_mask)?)
}

/// Project a RecordBatch according to the plan's projections.
pub fn project_batch(batch: &RecordBatch, plan: &SelectPlan) -> Result<RecordBatch, SelectError> {
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut fields: Vec<Field> = Vec::new();

    for (expr, alias) in &plan.projections {
        let arr = evaluate_expr(expr, batch)?;
        let dt = arr.data_type().clone();
        fields.push(Field::new(alias, dt, true));
        columns.push(arr);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
        .map_err(|e| SelectError::ExecutionError(format!("Projection failed: {e}")))
}

/// Execute an aggregate query, scanning all batches and returning a single-row result.
pub fn execute_aggregate(
    plan: &SelectPlan,
    batches: &[RecordBatch],
) -> Result<RecordBatch, SelectError> {
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut fields: Vec<Field> = Vec::new();

    for (expr, alias) in &plan.projections {
        let (arr, dt) = eval_aggregate_expr(expr, batches)?;
        fields.push(Field::new(alias, dt, true));
        columns.push(arr);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
        .map_err(|e| SelectError::ExecutionError(format!("Aggregate result failed: {e}")))
}

/// Evaluate an aggregate expression across all batches.
fn eval_aggregate_expr(
    expr: &ResolvedExpr,
    batches: &[RecordBatch],
) -> Result<(ArrayRef, DataType), SelectError> {
    match expr {
        ResolvedExpr::Aggregate { func, arg } => {
            functions::aggregate::compute_aggregate(func, arg, batches)
        }
        ResolvedExpr::Literal(scalar) => {
            let arr = scalar_to_array(scalar, 1)?;
            let dt = arr.data_type().clone();
            Ok((arr, dt))
        }
        _ => Err(SelectError::ExecutionError(
            "Non-aggregate expression in aggregate query".to_string(),
        )),
    }
}

/// Apply filter and projection to all batches (non-aggregate query).
pub fn execute_non_aggregate(
    plan: &SelectPlan,
    batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>, SelectError> {
    let mut result = Vec::new();

    for batch in batches {
        let filtered = if let Some(ref filter_expr) = plan.filter {
            filter_batch(batch, filter_expr)?
        } else {
            batch.clone()
        };

        if filtered.num_rows() == 0 {
            continue;
        }

        let projected = project_batch(&filtered, plan)?;
        result.push(projected);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("city", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
                Arc::new(Int64Array::from(vec![30, 25, 35])),
                Arc::new(StringArray::from(vec!["NYC", "LA", "Chicago"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_eval_column_ref() {
        let batch = test_batch();
        let expr = ResolvedExpr::ColumnIndex(1);
        let result = evaluate_expr(&expr, &batch).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_eval_literal() {
        let batch = test_batch();
        let expr = ResolvedExpr::Literal(ScalarValue::Int64(42));
        let result = evaluate_expr(&expr, &batch).unwrap();
        assert_eq!(result.len(), 3);
        let arr = result.as_primitive::<arrow::datatypes::Int64Type>();
        assert_eq!(arr.value(0), 42);
    }

    #[test]
    fn test_comparison_ops() {
        let batch = test_batch();
        // age > 28
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::ColumnIndex(1)),
            op: BinOp::Gt,
            right: Box::new(ResolvedExpr::Literal(ScalarValue::Int64(28))),
        };
        let result = evaluate_expr(&expr, &batch).unwrap();
        let bool_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_arr.value(0)); // 30 > 28
        assert!(!bool_arr.value(1)); // 25 > 28
        assert!(bool_arr.value(2)); // 35 > 28
    }

    #[test]
    fn test_filter_batch() {
        let batch = test_batch();
        let filter_expr = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::ColumnIndex(1)),
            op: BinOp::Gt,
            right: Box::new(ResolvedExpr::Literal(ScalarValue::Int64(28))),
        };
        let result = filter_batch(&batch, &filter_expr).unwrap();
        assert_eq!(result.num_rows(), 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_project_batch() {
        let batch = test_batch();
        let plan = SelectPlan {
            projections: vec![
                (ResolvedExpr::ColumnIndex(0), "name".to_string()),
                (ResolvedExpr::ColumnIndex(1), "age".to_string()),
            ],
            filter: None,
            has_aggregates: false,
        };
        let result = project_batch(&batch, &plan).unwrap();
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_is_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))],
        )
        .unwrap();
        let expr = ResolvedExpr::IsNull(Box::new(ResolvedExpr::ColumnIndex(0)));
        let result = evaluate_expr(&expr, &batch).unwrap();
        let bool_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bool_arr.value(0));
        assert!(bool_arr.value(1));
        assert!(!bool_arr.value(2));
    }

    #[test]
    fn test_between() {
        let batch = test_batch();
        let expr = ResolvedExpr::Between {
            expr: Box::new(ResolvedExpr::ColumnIndex(1)),
            low: Box::new(ResolvedExpr::Literal(ScalarValue::Int64(26))),
            high: Box::new(ResolvedExpr::Literal(ScalarValue::Int64(32))),
            negated: false,
        };
        let result = evaluate_expr(&expr, &batch).unwrap();
        let bool_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_arr.value(0)); // 30
        assert!(!bool_arr.value(1)); // 25
        assert!(!bool_arr.value(2)); // 35
    }

    #[test]
    fn test_in_list() {
        let batch = test_batch();
        let expr = ResolvedExpr::InList {
            expr: Box::new(ResolvedExpr::ColumnIndex(0)),
            list: vec![
                ResolvedExpr::Literal(ScalarValue::Utf8("Alice".to_string())),
                ResolvedExpr::Literal(ScalarValue::Utf8("Charlie".to_string())),
            ],
            negated: false,
        };
        let result = evaluate_expr(&expr, &batch).unwrap();
        let bool_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_arr.value(0)); // Alice
        assert!(!bool_arr.value(1)); // Bob
        assert!(bool_arr.value(2)); // Charlie
    }

    #[test]
    fn test_arithmetic_ops() {
        let batch = test_batch();
        // age * 2
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::ColumnIndex(1)),
            op: BinOp::Mul,
            right: Box::new(ResolvedExpr::Literal(ScalarValue::Int64(2))),
        };
        let result = evaluate_expr(&expr, &batch).unwrap();
        let arr = result.as_primitive::<arrow::datatypes::Int64Type>();
        assert_eq!(arr.value(0), 60);
        assert_eq!(arr.value(1), 50);
        assert_eq!(arr.value(2), 70);
    }
}
