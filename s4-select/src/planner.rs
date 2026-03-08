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

//! Query planner — converts a SQL AST into a [`SelectPlan`].
//!
//! Resolves column references against the Arrow schema, validates the SQL
//! subset supported by S3 Select, and produces a plan that the evaluator
//! can execute.

use arrow::datatypes::{DataType, SchemaRef};
use sqlparser::ast::{
    self, BinaryOperator, CastKind, DataType as SqlDataType, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, Query, Select, SelectItem, SetExpr, Statement, TrimWhereField,
    UnaryOperator, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::SelectError;

/// A planned SELECT query ready for execution.
#[derive(Debug, Clone)]
pub struct SelectPlan {
    /// Output columns: (expression, alias).
    pub projections: Vec<(ResolvedExpr, String)>,
    /// Optional WHERE filter.
    pub filter: Option<ResolvedExpr>,
    /// Whether any projection contains an aggregate function.
    pub has_aggregates: bool,
}

/// A resolved expression that can be evaluated against a RecordBatch.
#[derive(Debug, Clone)]
pub enum ResolvedExpr {
    /// Reference to a column by index.
    ColumnIndex(usize),
    /// Literal scalar value.
    Literal(ScalarValue),
    /// Binary operation (e.g. `a + b`, `a > b`, `a AND b`).
    BinaryOp {
        /// Left operand.
        left: Box<ResolvedExpr>,
        /// Operator.
        op: BinOp,
        /// Right operand.
        right: Box<ResolvedExpr>,
    },
    /// Unary operation (e.g. `NOT x`, `-x`).
    UnaryOp {
        /// Operator.
        op: UnOp,
        /// Operand.
        expr: Box<ResolvedExpr>,
    },
    /// Scalar function call.
    Function {
        /// Function name (uppercase).
        name: String,
        /// Arguments.
        args: Vec<ResolvedExpr>,
    },
    /// Aggregate function call.
    Aggregate {
        /// Aggregate function type.
        func: AggregateFunc,
        /// Argument (None for COUNT(*)).
        arg: Option<Box<ResolvedExpr>>,
    },
    /// IS NULL check.
    IsNull(Box<ResolvedExpr>),
    /// IS NOT NULL check.
    IsNotNull(Box<ResolvedExpr>),
    /// BETWEEN expression.
    Between {
        /// Expression to test.
        expr: Box<ResolvedExpr>,
        /// Low bound.
        low: Box<ResolvedExpr>,
        /// High bound.
        high: Box<ResolvedExpr>,
        /// Negated (NOT BETWEEN).
        negated: bool,
    },
    /// IN list expression.
    InList {
        /// Expression to test.
        expr: Box<ResolvedExpr>,
        /// List of values.
        list: Vec<ResolvedExpr>,
        /// Negated (NOT IN).
        negated: bool,
    },
    /// LIKE expression.
    Like {
        /// Expression to test.
        expr: Box<ResolvedExpr>,
        /// Pattern.
        pattern: Box<ResolvedExpr>,
        /// Negated (NOT LIKE).
        negated: bool,
    },
    /// CASE expression.
    Case {
        /// Optional operand for simple CASE.
        operand: Option<Box<ResolvedExpr>>,
        /// WHEN ... THEN ... pairs.
        when_then: Vec<(ResolvedExpr, ResolvedExpr)>,
        /// ELSE expression.
        else_expr: Option<Box<ResolvedExpr>>,
    },
    /// CAST expression.
    Cast {
        /// Expression to cast.
        expr: Box<ResolvedExpr>,
        /// Target Arrow data type.
        data_type: DataType,
    },
    /// Parenthesized expression.
    Nested(Box<ResolvedExpr>),
}

/// Scalar value types supported by S3 Select.
#[derive(Debug, Clone)]
pub enum ScalarValue {
    /// 64-bit integer.
    Int64(i64),
    /// 64-bit float.
    Float64(f64),
    /// UTF-8 string.
    Utf8(String),
    /// Boolean.
    Boolean(bool),
    /// NULL.
    Null,
}

/// Binary operators.
#[derive(Debug, Clone, Copy)]
pub enum BinOp {
    // Arithmetic
    /// Addition.
    Add,
    /// Subtraction.
    Sub,
    /// Multiplication.
    Mul,
    /// Division.
    Div,
    /// Modulo.
    Mod,
    // Comparison
    /// Equal.
    Eq,
    /// Not equal.
    Neq,
    /// Less than.
    Lt,
    /// Less than or equal.
    LtEq,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    GtEq,
    // Logical
    /// Logical AND.
    And,
    /// Logical OR.
    Or,
    // String
    /// String concatenation.
    Concat,
}

/// Unary operators.
#[derive(Debug, Clone, Copy)]
pub enum UnOp {
    /// Logical NOT.
    Not,
    /// Arithmetic negation.
    Minus,
}

/// Aggregate functions.
#[derive(Debug, Clone, Copy)]
pub enum AggregateFunc {
    /// COUNT(*).
    CountStar,
    /// COUNT(expr).
    Count,
    /// SUM.
    Sum,
    /// AVG.
    Avg,
    /// MIN.
    Min,
    /// MAX.
    Max,
}

/// Plan a SQL query against the given Arrow schema.
///
/// Parses the SQL, validates it conforms to the S3 Select subset,
/// resolves columns, and returns a [`SelectPlan`].
pub fn plan_query(sql: &str, schema: &SchemaRef) -> Result<SelectPlan, SelectError> {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| SelectError::InvalidExpression(format!("SQL parse error: {e}")))?;

    if stmts.len() != 1 {
        return Err(SelectError::InvalidExpression(
            "Expected exactly one SQL statement".to_string(),
        ));
    }

    let query = match &stmts[0] {
        Statement::Query(q) => q,
        _ => {
            return Err(SelectError::NonSelectStatement(
                "Only SELECT statements are supported".to_string(),
            ))
        }
    };

    plan_query_ast(query, schema)
}

fn plan_query_ast(query: &Query, schema: &SchemaRef) -> Result<SelectPlan, SelectError> {
    // Reject ORDER BY, LIMIT, OFFSET
    if query.order_by.is_some() {
        return Err(SelectError::InvalidExpression(
            "ORDER BY is not supported in S3 Select".to_string(),
        ));
    }
    if query.limit.is_some() {
        return Err(SelectError::InvalidExpression(
            "LIMIT is not supported in S3 Select".to_string(),
        ));
    }
    if query.offset.is_some() {
        return Err(SelectError::InvalidExpression(
            "OFFSET is not supported in S3 Select".to_string(),
        ));
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        SetExpr::SetOperation { .. } => {
            return Err(SelectError::InvalidExpression(
                "UNION/INTERSECT/EXCEPT is not supported in S3 Select".to_string(),
            ))
        }
        _ => {
            return Err(SelectError::InvalidExpression(
                "Only simple SELECT is supported".to_string(),
            ))
        }
    };

    plan_select(select, schema)
}

fn plan_select(select: &Select, schema: &SchemaRef) -> Result<SelectPlan, SelectError> {
    // Reject GROUP BY, HAVING
    if select.group_by != ast::GroupByExpr::Expressions(vec![], vec![]) {
        return Err(SelectError::InvalidExpression(
            "GROUP BY is not supported in S3 Select".to_string(),
        ));
    }
    if select.having.is_some() {
        return Err(SelectError::InvalidExpression(
            "HAVING is not supported in S3 Select".to_string(),
        ));
    }

    // Resolve projections
    let mut projections = Vec::new();
    let mut has_aggregates = false;

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let resolved = resolve_expr(expr, schema)?;
                let alias = expr_default_name(expr);
                if contains_aggregate(&resolved) {
                    has_aggregates = true;
                }
                projections.push((resolved, alias));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let resolved = resolve_expr(expr, schema)?;
                if contains_aggregate(&resolved) {
                    has_aggregates = true;
                }
                projections.push((resolved, alias.value.clone()));
            }
            SelectItem::Wildcard(_) => {
                for (i, field) in schema.fields().iter().enumerate() {
                    projections.push((ResolvedExpr::ColumnIndex(i), field.name().clone()));
                }
            }
            SelectItem::QualifiedWildcard(_, _) => {
                // Treat qualified wildcard same as wildcard for single-table S3 Select
                for (i, field) in schema.fields().iter().enumerate() {
                    projections.push((ResolvedExpr::ColumnIndex(i), field.name().clone()));
                }
            }
        }
    }

    // Resolve filter
    let filter = select.selection.as_ref().map(|expr| resolve_expr(expr, schema)).transpose()?;

    // Validate: if aggregates present, no bare column refs allowed in projections
    if has_aggregates {
        for (expr, _) in &projections {
            validate_no_bare_columns(expr)?;
        }
    }

    Ok(SelectPlan {
        projections,
        filter,
        has_aggregates,
    })
}

/// Resolve a SQL expression against the schema.
fn resolve_expr(expr: &Expr, schema: &SchemaRef) -> Result<ResolvedExpr, SelectError> {
    match expr {
        Expr::Identifier(ident) => resolve_column_name(&ident.value, schema),

        Expr::CompoundIdentifier(parts) => {
            // e.g. s3object.name or alias.column — use the last part
            let col_name = &parts.last().unwrap().value;
            resolve_column_name(col_name, schema)
        }

        Expr::Value(ref value_with_span) => resolve_value(&value_with_span.value),

        Expr::BinaryOp { left, op, right } => {
            let left = resolve_expr(left, schema)?;
            let right = resolve_expr(right, schema)?;
            let op = resolve_binary_op(op)?;
            Ok(ResolvedExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }

        Expr::UnaryOp { op, expr } => {
            let expr = resolve_expr(expr, schema)?;
            let op = match op {
                UnaryOperator::Not => UnOp::Not,
                UnaryOperator::Minus => UnOp::Minus,
                other => {
                    return Err(SelectError::InvalidExpression(format!(
                        "Unsupported unary operator: {other}"
                    )))
                }
            };
            Ok(ResolvedExpr::UnaryOp {
                op,
                expr: Box::new(expr),
            })
        }

        Expr::Nested(inner) => {
            let inner = resolve_expr(inner, schema)?;
            Ok(ResolvedExpr::Nested(Box::new(inner)))
        }

        Expr::IsNull(inner) => {
            let inner = resolve_expr(inner, schema)?;
            Ok(ResolvedExpr::IsNull(Box::new(inner)))
        }

        Expr::IsNotNull(inner) => {
            let inner = resolve_expr(inner, schema)?;
            Ok(ResolvedExpr::IsNotNull(Box::new(inner)))
        }

        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let expr = resolve_expr(expr, schema)?;
            let low = resolve_expr(low, schema)?;
            let high = resolve_expr(high, schema)?;
            Ok(ResolvedExpr::Between {
                expr: Box::new(expr),
                low: Box::new(low),
                high: Box::new(high),
                negated: *negated,
            })
        }

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = resolve_expr(expr, schema)?;
            let list =
                list.iter().map(|e| resolve_expr(e, schema)).collect::<Result<Vec<_>, _>>()?;
            Ok(ResolvedExpr::InList {
                expr: Box::new(expr),
                list,
                negated: *negated,
            })
        }

        Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => {
            let expr = resolve_expr(expr, schema)?;
            let pattern = resolve_expr(pattern, schema)?;
            Ok(ResolvedExpr::Like {
                expr: Box::new(expr),
                pattern: Box::new(pattern),
                negated: *negated,
            })
        }

        Expr::ILike {
            expr,
            pattern,
            negated,
            ..
        } => {
            // Treat ILIKE as LIKE (case-insensitive handled by wrapping in LOWER)
            let expr = resolve_expr(expr, schema)?;
            let pattern = resolve_expr(pattern, schema)?;
            let lower_expr = ResolvedExpr::Function {
                name: "LOWER".to_string(),
                args: vec![expr],
            };
            let lower_pattern = ResolvedExpr::Function {
                name: "LOWER".to_string(),
                args: vec![pattern],
            };
            Ok(ResolvedExpr::Like {
                expr: Box::new(lower_expr),
                pattern: Box::new(lower_pattern),
                negated: *negated,
            })
        }

        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let operand =
                operand.as_ref().map(|e| resolve_expr(e, schema).map(Box::new)).transpose()?;
            let when_then = conditions
                .iter()
                .map(|cw| {
                    Ok((
                        resolve_expr(&cw.condition, schema)?,
                        resolve_expr(&cw.result, schema)?,
                    ))
                })
                .collect::<Result<Vec<_>, SelectError>>()?;
            let else_expr = else_result
                .as_ref()
                .map(|e| resolve_expr(e, schema).map(Box::new))
                .transpose()?;
            Ok(ResolvedExpr::Case {
                operand,
                when_then,
                else_expr,
            })
        }

        Expr::Cast {
            expr,
            data_type,
            kind,
            ..
        } => {
            if *kind != CastKind::Cast {
                return Err(SelectError::InvalidExpression(
                    "Only CAST is supported (not TRY_CAST)".to_string(),
                ));
            }
            let expr = resolve_expr(expr, schema)?;
            let arrow_type = sql_type_to_arrow(data_type)?;
            Ok(ResolvedExpr::Cast {
                expr: Box::new(expr),
                data_type: arrow_type,
            })
        }

        Expr::Function(func) => resolve_function(func, schema),

        Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            let expr_resolved = resolve_expr(expr, schema)?;
            // For simple TRIM, we map to our TRIM function
            let trim_char = if let Some(what) = trim_what {
                if let Expr::Value(ref vws) = what.as_ref() {
                    if let Value::SingleQuotedString(s) = &vws.value {
                        Some(s.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else if let Some(chars) = trim_characters {
                if let Some(Expr::Value(ref vws)) = chars.first() {
                    if let Value::SingleQuotedString(s) = &vws.value {
                        Some(s.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };
            let where_part = match trim_where {
                Some(TrimWhereField::Leading) => "LEADING",
                Some(TrimWhereField::Trailing) => "TRAILING",
                Some(TrimWhereField::Both) | None => "BOTH",
            };
            let mut args = vec![expr_resolved];
            args.push(ResolvedExpr::Literal(ScalarValue::Utf8(
                where_part.to_string(),
            )));
            if let Some(c) = trim_char {
                args.push(ResolvedExpr::Literal(ScalarValue::Utf8(c)));
            }
            Ok(ResolvedExpr::Function {
                name: "TRIM".to_string(),
                args,
            })
        }

        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let expr_resolved = resolve_expr(expr, schema)?;
            let mut args = vec![expr_resolved];
            if let Some(from) = substring_from {
                args.push(resolve_expr(from, schema)?);
            }
            if let Some(for_expr) = substring_for {
                args.push(resolve_expr(for_expr, schema)?);
            }
            Ok(ResolvedExpr::Function {
                name: "SUBSTRING".to_string(),
                args,
            })
        }

        _ => Err(SelectError::InvalidExpression(format!(
            "Unsupported expression: {expr}"
        ))),
    }
}

/// Resolve a column name to a column index.
fn resolve_column_name(name: &str, schema: &SchemaRef) -> Result<ResolvedExpr, SelectError> {
    // Check for positional columns (_1, _2, etc.)
    if let Some(stripped) = name.strip_prefix('_') {
        if let Ok(pos) = stripped.parse::<usize>() {
            if pos >= 1 && pos <= schema.fields().len() {
                return Ok(ResolvedExpr::ColumnIndex(pos - 1));
            }
        }
    }

    // Case-insensitive name lookup
    let lower = name.to_lowercase();
    for (i, field) in schema.fields().iter().enumerate() {
        if field.name().to_lowercase() == lower {
            return Ok(ResolvedExpr::ColumnIndex(i));
        }
    }

    // Handle IS MISSING as alias for NULL check (not a column)
    // This is handled at the expression level, not here

    Err(SelectError::InvalidExpression(format!(
        "Column '{}' not found in schema. Available: {}",
        name,
        schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", ")
    )))
}

/// Resolve a SQL literal value.
fn resolve_value(value: &Value) -> Result<ResolvedExpr, SelectError> {
    match value {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(ResolvedExpr::Literal(ScalarValue::Int64(i)))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(ResolvedExpr::Literal(ScalarValue::Float64(f)))
            } else {
                Err(SelectError::InvalidExpression(format!(
                    "Invalid number: {n}"
                )))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(ResolvedExpr::Literal(ScalarValue::Utf8(s.clone())))
        }
        Value::Boolean(b) => Ok(ResolvedExpr::Literal(ScalarValue::Boolean(*b))),
        Value::Null => Ok(ResolvedExpr::Literal(ScalarValue::Null)),
        _ => Err(SelectError::InvalidExpression(format!(
            "Unsupported literal value: {value}"
        ))),
    }
}

/// Resolve a binary operator.
fn resolve_binary_op(op: &BinaryOperator) -> Result<BinOp, SelectError> {
    match op {
        BinaryOperator::Plus => Ok(BinOp::Add),
        BinaryOperator::Minus => Ok(BinOp::Sub),
        BinaryOperator::Multiply => Ok(BinOp::Mul),
        BinaryOperator::Divide => Ok(BinOp::Div),
        BinaryOperator::Modulo => Ok(BinOp::Mod),
        BinaryOperator::Eq => Ok(BinOp::Eq),
        BinaryOperator::NotEq => Ok(BinOp::Neq),
        BinaryOperator::Lt => Ok(BinOp::Lt),
        BinaryOperator::LtEq => Ok(BinOp::LtEq),
        BinaryOperator::Gt => Ok(BinOp::Gt),
        BinaryOperator::GtEq => Ok(BinOp::GtEq),
        BinaryOperator::And => Ok(BinOp::And),
        BinaryOperator::Or => Ok(BinOp::Or),
        BinaryOperator::StringConcat => Ok(BinOp::Concat),
        _ => Err(SelectError::InvalidExpression(format!(
            "Unsupported binary operator: {op}"
        ))),
    }
}

/// Resolve a SQL function call.
fn resolve_function(func: &ast::Function, schema: &SchemaRef) -> Result<ResolvedExpr, SelectError> {
    let name = func.name.to_string().to_uppercase();

    // Check for aggregate functions
    match name.as_str() {
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
            return resolve_aggregate_function(&name, func, schema);
        }
        _ => {}
    }

    // Scalar function — resolve arguments
    let args = resolve_function_args(&func.args, schema)?;

    Ok(ResolvedExpr::Function { name, args })
}

/// Resolve an aggregate function.
fn resolve_aggregate_function(
    name: &str,
    func: &ast::Function,
    schema: &SchemaRef,
) -> Result<ResolvedExpr, SelectError> {
    let args = resolve_function_args(&func.args, schema)?;

    let (agg_func, arg) = match name {
        "COUNT" => {
            if args.is_empty() {
                // COUNT(*) — detected by wildcard
                (AggregateFunc::CountStar, None)
            } else {
                // Check if it's COUNT(*)
                if is_count_star(func) {
                    (AggregateFunc::CountStar, None)
                } else {
                    (AggregateFunc::Count, Some(Box::new(args[0].clone())))
                }
            }
        }
        "SUM" => {
            if args.is_empty() {
                return Err(SelectError::InvalidExpression(
                    "SUM requires an argument".to_string(),
                ));
            }
            (AggregateFunc::Sum, Some(Box::new(args[0].clone())))
        }
        "AVG" => {
            if args.is_empty() {
                return Err(SelectError::InvalidExpression(
                    "AVG requires an argument".to_string(),
                ));
            }
            (AggregateFunc::Avg, Some(Box::new(args[0].clone())))
        }
        "MIN" => {
            if args.is_empty() {
                return Err(SelectError::InvalidExpression(
                    "MIN requires an argument".to_string(),
                ));
            }
            (AggregateFunc::Min, Some(Box::new(args[0].clone())))
        }
        "MAX" => {
            if args.is_empty() {
                return Err(SelectError::InvalidExpression(
                    "MAX requires an argument".to_string(),
                ));
            }
            (AggregateFunc::Max, Some(Box::new(args[0].clone())))
        }
        _ => unreachable!(),
    };

    Ok(ResolvedExpr::Aggregate {
        func: agg_func,
        arg,
    })
}

/// Check if a function call is COUNT(*).
fn is_count_star(func: &ast::Function) -> bool {
    match &func.args {
        FunctionArguments::None => false,
        FunctionArguments::Subquery(_) => false,
        FunctionArguments::List(arg_list) => {
            if arg_list.args.is_empty() {
                return false;
            }
            matches!(
                &arg_list.args[0],
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
            )
        }
    }
}

/// Resolve function arguments to expressions.
fn resolve_function_args(
    args: &FunctionArguments,
    schema: &SchemaRef,
) -> Result<Vec<ResolvedExpr>, SelectError> {
    match args {
        FunctionArguments::None => Ok(vec![]),
        FunctionArguments::Subquery(_) => Err(SelectError::InvalidExpression(
            "Subqueries are not supported".to_string(),
        )),
        FunctionArguments::List(arg_list) => {
            let mut resolved = Vec::new();
            for arg in &arg_list.args {
                match arg {
                    FunctionArg::Named { arg, .. }
                    | FunctionArg::Unnamed(arg)
                    | FunctionArg::ExprNamed { arg, .. } => match arg {
                        FunctionArgExpr::Expr(e) => {
                            resolved.push(resolve_expr(e, schema)?);
                        }
                        FunctionArgExpr::Wildcard => {
                            // * argument (e.g. COUNT(*)) — skip
                        }
                        FunctionArgExpr::QualifiedWildcard(_) => {
                            // qualified.* — skip
                        }
                    },
                }
            }
            Ok(resolved)
        }
    }
}

/// Check if expression contains an aggregate function.
fn contains_aggregate(expr: &ResolvedExpr) -> bool {
    match expr {
        ResolvedExpr::Aggregate { .. } => true,
        ResolvedExpr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        ResolvedExpr::UnaryOp { expr, .. } => contains_aggregate(expr),
        ResolvedExpr::Nested(inner) => contains_aggregate(inner),
        ResolvedExpr::Function { args, .. } => args.iter().any(contains_aggregate),
        ResolvedExpr::Cast { expr, .. } => contains_aggregate(expr),
        ResolvedExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.as_ref().is_some_and(|e| contains_aggregate(e))
                || when_then.iter().any(|(c, r)| contains_aggregate(c) || contains_aggregate(r))
                || else_expr.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        _ => false,
    }
}

/// Validate that an expression does not contain bare column references
/// (only allowed inside aggregates).
fn validate_no_bare_columns(expr: &ResolvedExpr) -> Result<(), SelectError> {
    match expr {
        ResolvedExpr::ColumnIndex(_) => Err(SelectError::InvalidExpression(
            "Column reference outside aggregate function in aggregate query".to_string(),
        )),
        ResolvedExpr::Aggregate { .. } => Ok(()), // Aggregates can contain column refs
        ResolvedExpr::BinaryOp { left, right, .. } => {
            validate_no_bare_columns(left)?;
            validate_no_bare_columns(right)
        }
        ResolvedExpr::UnaryOp { expr, .. } => validate_no_bare_columns(expr),
        ResolvedExpr::Nested(inner) => validate_no_bare_columns(inner),
        ResolvedExpr::Function { args, .. } => {
            for arg in args {
                validate_no_bare_columns(arg)?;
            }
            Ok(())
        }
        ResolvedExpr::Literal(_) => Ok(()),
        ResolvedExpr::Cast { expr, .. } => validate_no_bare_columns(expr),
        _ => Ok(()),
    }
}

/// Generate a default column name for an expression.
fn expr_default_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts.last().unwrap().value.clone(),
        Expr::Function(func) => func.to_string(),
        _ => expr.to_string(),
    }
}

/// Convert a SQL data type to an Arrow data type.
fn sql_type_to_arrow(sql_type: &SqlDataType) -> Result<DataType, SelectError> {
    match sql_type {
        SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Boolean),
        SqlDataType::Int(None)
        | SqlDataType::Integer(None)
        | SqlDataType::BigInt(None)
        | SqlDataType::Int64 => Ok(DataType::Int64),
        SqlDataType::SmallInt(None) | SqlDataType::Int2(_) => Ok(DataType::Int16),
        SqlDataType::Int4(_) | SqlDataType::Int(Some(_)) | SqlDataType::Integer(Some(_)) => {
            Ok(DataType::Int32)
        }
        SqlDataType::Float(None) | SqlDataType::Real => Ok(DataType::Float32),
        SqlDataType::Double(_) | SqlDataType::DoublePrecision | SqlDataType::Float64 => {
            Ok(DataType::Float64)
        }
        SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::String(_)
        | SqlDataType::Char(_)
        | SqlDataType::CharVarying(_) => Ok(DataType::Utf8),
        SqlDataType::Timestamp(_, _) => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Decimal(_) | SqlDataType::Numeric(_) | SqlDataType::Dec(_) => {
            Ok(DataType::Float64) // Simplification: treat DECIMAL as Float64
        }
        _ => Err(SelectError::InvalidExpression(format!(
            "Unsupported data type for CAST: {sql_type}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("city", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_plan_select_star() {
        let schema = test_schema();
        let plan = plan_query("SELECT * FROM s3object", &schema).unwrap();
        assert_eq!(plan.projections.len(), 3);
        assert!(!plan.has_aggregates);
        assert!(plan.filter.is_none());
    }

    #[test]
    fn test_plan_with_where() {
        let schema = test_schema();
        let plan = plan_query("SELECT name FROM s3object WHERE age > 25", &schema).unwrap();
        assert_eq!(plan.projections.len(), 1);
        assert!(plan.filter.is_some());
    }

    #[test]
    fn test_plan_with_aggregates() {
        let schema = test_schema();
        let plan = plan_query("SELECT COUNT(*) FROM s3object", &schema).unwrap();
        assert!(plan.has_aggregates);
        assert_eq!(plan.projections.len(), 1);
    }

    #[test]
    fn test_plan_positional_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_1", DataType::Utf8, true),
            Field::new("_2", DataType::Int64, true),
        ]));
        let plan = plan_query("SELECT _1, _2 FROM s3object", &schema).unwrap();
        assert_eq!(plan.projections.len(), 2);
    }

    #[test]
    fn test_reject_order_by() {
        let schema = test_schema();
        let result = plan_query("SELECT * FROM s3object ORDER BY name", &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_case_insensitive_columns() {
        let schema = test_schema();
        let plan = plan_query("SELECT NAME, AGE FROM s3object", &schema).unwrap();
        assert_eq!(plan.projections.len(), 2);
    }

    #[test]
    fn test_plan_with_alias() {
        let schema = test_schema();
        let plan = plan_query("SELECT name as n, age as a FROM s3object", &schema).unwrap();
        assert_eq!(plan.projections[0].1, "n");
        assert_eq!(plan.projections[1].1, "a");
    }

    #[test]
    fn test_reject_group_by() {
        let schema = test_schema();
        let result = plan_query("SELECT city, COUNT(*) FROM s3object GROUP BY city", &schema);
        assert!(result.is_err());
    }
}
