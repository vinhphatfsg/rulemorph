//! v2 Evaluation Context and Functions for rulemorph v2.0
//!
//! This module provides the evaluation context and functions for v2 expressions,
//! including pipe value tracking, let bindings, and item/acc scopes.

use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::{V2Expr, V2OpStep, V2Pipe, V2Start};

mod cast;
mod collection;
mod comparison;
mod condition;
mod context;
mod control;
mod lookup;
mod reference;
#[cfg(test)]
mod tests;
mod v1_bridge;

use cast::eval_type_cast;
use collection::eval_collection_op;
use comparison::eval_comparison_op;
pub use condition::eval_v2_condition;
pub use context::{EvalItem, EvalValue, V2EvalContext};
pub use control::{
    eval_v2_expr, eval_v2_if_step, eval_v2_let_step, eval_v2_map_step, eval_v2_pipe,
};
use lookup::eval_lookup_op;
pub use reference::{eval_v2_ref, eval_v2_start};
use v1_bridge::eval_v2_op_with_v1_fallback;

// =============================================================================
// v2 Reference Evaluation (T13)
// =============================================================================

// =============================================================================
// v2 Start Value Evaluation (T14)
// =============================================================================

// =============================================================================
// v2 Op Step Evaluation (T15)
// =============================================================================

/// Helper to convert EvalValue to string
fn eval_value_as_string(value: &EvalValue, path: &str) -> Result<String, TransformError> {
    match value {
        EvalValue::Missing => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expected string, got missing value",
        )
        .with_path(path)),
        EvalValue::Value(v) => match v {
            JsonValue::String(s) => Ok(s.clone()),
            JsonValue::Number(n) => Ok(n.to_string()),
            JsonValue::Bool(b) => Ok(b.to_string()),
            _ => Err(TransformError::new(
                TransformErrorKind::ExprError,
                format!("expected string, got {:?}", v),
            )
            .with_path(path)),
        },
    }
}

/// Helper to convert EvalValue to number
fn eval_value_as_number(value: &EvalValue, path: &str) -> Result<f64, TransformError> {
    match value {
        EvalValue::Missing => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expected number, got missing value",
        )
        .with_path(path)),
        EvalValue::Value(v) => match v {
            JsonValue::Number(n) => n.as_f64().ok_or_else(|| {
                TransformError::new(TransformErrorKind::ExprError, "number conversion failed")
                    .with_path(path)
            }),
            JsonValue::String(s) => s.parse::<f64>().map_err(|_| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "failed to parse string as number",
                )
                .with_path(path)
            }),
            _ => Err(TransformError::new(
                TransformErrorKind::ExprError,
                format!("expected number, got {:?}", v),
            )
            .with_path(path)),
        },
    }
}

fn value_as_bool(value: &JsonValue, path: &str) -> Result<bool, TransformError> {
    match value {
        JsonValue::Bool(flag) => Ok(*flag),
        _ => Err(
            TransformError::new(TransformErrorKind::ExprError, "value must be a boolean")
                .with_path(path),
        ),
    }
}

fn eval_v2_expr_or_null<'a>(
    expr: &V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<JsonValue, TransformError> {
    match eval_v2_expr(expr, record, context, out, path, ctx)? {
        EvalValue::Missing => Ok(JsonValue::Null),
        EvalValue::Value(value) => Ok(value),
    }
}

fn number_to_string(number: &serde_json::Number) -> String {
    if let Some(i) = number.as_i64() {
        return i.to_string();
    }
    if let Some(u) = number.as_u64() {
        return u.to_string();
    }
    if let Some(f) = number.as_f64() {
        let mut s = format!("{}", f);
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        return s;
    }
    number.to_string()
}

fn value_to_string(value: &JsonValue, path: &str) -> Result<String, TransformError> {
    match value {
        JsonValue::String(s) => Ok(s.clone()),
        JsonValue::Number(n) => Ok(number_to_string(n)),
        JsonValue::Bool(b) => Ok(b.to_string()),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "value must be string/number/bool",
        )
        .with_path(path)),
    }
}

/// Evaluate a v2 op step with a pipe value as implicit first argument
pub fn eval_v2_op_step<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Create a new context with the current pipe value
    let step_ctx = ctx.clone().with_pipe_value(pipe_value.clone());

    // Handle "@..." as a reference (from shorthand string in step position)
    if op_step.op.starts_with('@') {
        use crate::v2_parser::parse_v2_ref;
        if let Some(v2_ref) = parse_v2_ref(&op_step.op) {
            return eval_v2_ref(&v2_ref, record, context, out, path, &step_ctx);
        }
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid reference: {}", op_step.op),
        )
        .with_path(path));
    }

    match op_step.op.as_str() {
        // String operations
        "trim" => {
            if matches!(pipe_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }
            let s = eval_value_as_string(&pipe_value, path)?;
            Ok(EvalValue::Value(JsonValue::String(s.trim().to_string())))
        }
        "lowercase" => {
            if matches!(pipe_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }
            let s = eval_value_as_string(&pipe_value, path)?;
            Ok(EvalValue::Value(JsonValue::String(s.to_lowercase())))
        }
        "uppercase" => {
            if matches!(pipe_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }
            let s = eval_value_as_string(&pipe_value, path)?;
            Ok(EvalValue::Value(JsonValue::String(s.to_uppercase())))
        }
        "to_string" => match &pipe_value {
            EvalValue::Missing => Ok(EvalValue::Missing),
            EvalValue::Value(v) => {
                let s = match v {
                    JsonValue::String(s) => s.clone(),
                    JsonValue::Number(n) => n.to_string(),
                    JsonValue::Bool(b) => b.to_string(),
                    JsonValue::Null => "null".to_string(),
                    JsonValue::Array(_) | JsonValue::Object(_) => v.to_string(),
                };
                Ok(EvalValue::Value(JsonValue::String(s)))
            }
        },
        "concat" => {
            // Pipe value is first, then args
            let mut parts = Vec::new();
            if matches!(pipe_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }
            parts.push(eval_value_as_string(&pipe_value, path)?);
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                if matches!(arg_value, EvalValue::Missing) {
                    return Ok(EvalValue::Missing);
                }
                parts.push(eval_value_as_string(&arg_value, &arg_path)?);
            }
            Ok(EvalValue::Value(JsonValue::String(parts.join(""))))
        }
        "string" | "int" | "float" | "bool" => {
            eval_type_cast(op_step.op.as_str(), &pipe_value, path)
        }

        // Numeric operations
        "add" | "+" => {
            if matches!(pipe_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }
            let mut result = eval_value_as_number(&pipe_value, path)?;
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                if matches!(arg_value, EvalValue::Missing) {
                    return Ok(EvalValue::Missing);
                }
                result += eval_value_as_number(&arg_value, &arg_path)?;
            }
            Ok(EvalValue::Value(serde_json::json!(result)))
        }
        "subtract" | "-" => {
            if op_step.args.is_empty() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "subtract requires at least one argument",
                )
                .with_path(path));
            }
            if matches!(pipe_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }
            let mut result = eval_value_as_number(&pipe_value, path)?;
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                if matches!(arg_value, EvalValue::Missing) {
                    return Ok(EvalValue::Missing);
                }
                result -= eval_value_as_number(&arg_value, &arg_path)?;
            }
            Ok(EvalValue::Value(serde_json::json!(result)))
        }
        "multiply" | "*" => {
            if matches!(pipe_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }
            let mut result = eval_value_as_number(&pipe_value, path)?;
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                if matches!(arg_value, EvalValue::Missing) {
                    return Ok(EvalValue::Missing);
                }
                result *= eval_value_as_number(&arg_value, &arg_path)?;
            }
            Ok(EvalValue::Value(serde_json::json!(result)))
        }
        "divide" | "/" => {
            if op_step.args.is_empty() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "divide requires at least one argument",
                )
                .with_path(path));
            }
            if matches!(pipe_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }
            let mut result = eval_value_as_number(&pipe_value, path)?;
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                if matches!(arg_value, EvalValue::Missing) {
                    return Ok(EvalValue::Missing);
                }
                let divisor = eval_value_as_number(&arg_value, &arg_path)?;
                if divisor == 0.0 {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "division by zero",
                    )
                    .with_path(&arg_path));
                }
                result /= divisor;
            }
            Ok(EvalValue::Value(serde_json::json!(result)))
        }
        "map" | "filter" | "flat_map" | "group_by" | "key_by" | "partition" | "distinct_by"
        | "sort_by" | "find" | "find_index" | "reduce" | "fold" | "zip_with" => {
            eval_collection_op(op_step, pipe_value, record, context, out, path, &step_ctx)
        }
        "first" => match &pipe_value {
            EvalValue::Missing => Ok(EvalValue::Missing),
            EvalValue::Value(JsonValue::Array(arr)) => {
                if let Some(value) = arr.first() {
                    Ok(EvalValue::Value(value.clone()))
                } else {
                    Ok(EvalValue::Missing)
                }
            }
            EvalValue::Value(other) => Err(TransformError::new(
                TransformErrorKind::ExprError,
                format!("first requires array, got {:?}", other),
            )
            .with_path(path)),
        },
        "last" => match &pipe_value {
            EvalValue::Missing => Ok(EvalValue::Missing),
            EvalValue::Value(JsonValue::Array(arr)) => {
                if let Some(value) = arr.last() {
                    Ok(EvalValue::Value(value.clone()))
                } else {
                    Ok(EvalValue::Missing)
                }
            }
            EvalValue::Value(other) => Err(TransformError::new(
                TransformErrorKind::ExprError,
                format!("last requires array, got {:?}", other),
            )
            .with_path(path)),
        },

        // Coalesce
        "coalesce" => {
            // If pipe value is present and not null, use it
            if let EvalValue::Value(v) = &pipe_value {
                if !v.is_null() {
                    return Ok(pipe_value);
                }
            }
            // Otherwise, try args in order
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                if let EvalValue::Value(v) = &arg_value {
                    if !v.is_null() {
                        return Ok(arg_value);
                    }
                }
            }
            Ok(EvalValue::Missing)
        }
        "and" | "or" => {
            let is_and = op_step.op == "and";
            let total_len = op_step.args.len() + 1;
            if total_len < 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr.args must contain at least two items",
                )
                .with_path(format!("{}.args", path)));
            }

            let mut saw_missing = false;
            match &pipe_value {
                EvalValue::Missing => saw_missing = true,
                EvalValue::Value(value) => {
                    let flag = value_as_bool(value, path)?;
                    if is_and {
                        if !flag {
                            return Ok(EvalValue::Value(JsonValue::Bool(false)));
                        }
                    } else if flag {
                        return Ok(EvalValue::Value(JsonValue::Bool(true)));
                    }
                }
            }

            for (index, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, index);
                let value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                match value {
                    EvalValue::Missing => {
                        saw_missing = true;
                        continue;
                    }
                    EvalValue::Value(value) => {
                        let flag = value_as_bool(&value, &arg_path)?;
                        if is_and {
                            if !flag {
                                return Ok(EvalValue::Value(JsonValue::Bool(false)));
                            }
                        } else if flag {
                            return Ok(EvalValue::Value(JsonValue::Bool(true)));
                        }
                    }
                }
            }

            if saw_missing {
                Ok(EvalValue::Missing)
            } else {
                Ok(EvalValue::Value(JsonValue::Bool(is_and)))
            }
        }
        "not" => {
            if !op_step.args.is_empty() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr.args must contain exactly one item",
                )
                .with_path(format!("{}.args", path)));
            }
            match pipe_value {
                EvalValue::Missing => Ok(EvalValue::Missing),
                EvalValue::Value(value) => {
                    let flag = value_as_bool(&value, path)?;
                    Ok(EvalValue::Value(JsonValue::Bool(!flag)))
                }
            }
        }
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" | "eq" | "ne" | "lt" | "lte" | "gt"
        | "gte" | "match" => {
            eval_comparison_op(op_step, pipe_value, record, context, out, path, &step_ctx)
        }
        "pick" | "omit" => {
            if op_step.args.is_empty() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("{} requires at least one argument", op_step.op),
                )
                .with_path(format!("{}.args", path)));
            }

            let mut path_values = Vec::new();
            for (index, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, index);
                let value = match eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)? {
                    EvalValue::Missing => return Ok(EvalValue::Missing),
                    EvalValue::Value(value) => value,
                };
                if value.is_null() {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "expr arg must not be null",
                    )
                    .with_path(arg_path));
                }
                match value {
                    JsonValue::String(path_value) => {
                        path_values.push(JsonValue::String(path_value));
                    }
                    JsonValue::Array(items) => {
                        for (item_index, item) in items.iter().enumerate() {
                            let item_path = format!("{}.args[{}][{}]", path, index, item_index);
                            let path_value = item.as_str().ok_or_else(|| {
                                TransformError::new(
                                    TransformErrorKind::ExprError,
                                    "paths must be a string or array of strings",
                                )
                                .with_path(item_path)
                            })?;
                            path_values.push(JsonValue::String(path_value.to_string()));
                        }
                    }
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "paths must be a string or array of strings",
                        )
                        .with_path(arg_path));
                    }
                }
            }

            let normalized_op = V2OpStep {
                op: op_step.op.clone(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(JsonValue::Array(path_values)),
                    steps: vec![],
                })],
            };
            eval_v2_op_with_v1_fallback(
                &normalized_op,
                pipe_value,
                record,
                context,
                out,
                path,
                &step_ctx,
            )
        }

        "lookup_first" | "lookup" => {
            eval_lookup_op(op_step, pipe_value, record, context, out, path, &step_ctx)
        }

        // Default case - fall back to v1 op evaluation
        _ => {
            eval_v2_op_with_v1_fallback(op_step, pipe_value, record, context, out, path, &step_ctx)
        }
    }
}
