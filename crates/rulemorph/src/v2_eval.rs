//! v2 Evaluation Context and Functions for rulemorph v2.0
//!
//! This module provides the evaluation context and functions for v2 expressions,
//! including pipe value tracking, let bindings, and item/acc scopes.

use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{Expr, ExprOp, ExprRef};
use crate::transform::{
    EvalItem as V1EvalItem, EvalLocals as V1EvalLocals, EvalValue as V1EvalValue,
    eval_op as eval_v1_op,
};
use crate::v2_model::{V2Expr, V2IfStep, V2LetStep, V2MapStep, V2OpStep, V2Pipe, V2Start, V2Step};

mod condition;
mod context;
mod reference;
#[cfg(test)]
mod tests;

use condition::compare_values_eq;
pub use condition::eval_v2_condition;
pub use context::{EvalItem, EvalValue, V2EvalContext};
pub use reference::{eval_v2_ref, eval_v2_start};

// =============================================================================
// v2 Reference Evaluation (T13)
// =============================================================================

// =============================================================================
// v2 Start Value Evaluation (T14)
// =============================================================================

// =============================================================================
// v2 Op Step Evaluation (T15)
// =============================================================================

/// Evaluate a v2 pipe expression
pub fn eval_v2_pipe<'a>(
    pipe: &V2Pipe,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Evaluate start value
    let mut current = eval_v2_start(&pipe.start, record, context, out, path, ctx)?;
    let mut current_ctx = ctx.clone();

    // Apply each step
    for (i, step) in pipe.steps.iter().enumerate() {
        let step_path = format!("{}[{}]", path, i + 1);
        // Update context with current pipe value for each step
        current_ctx = current_ctx.clone().with_pipe_value(current.clone());

        match step {
            V2Step::Op(op_step) => {
                current = eval_v2_op_step(
                    op_step,
                    current,
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                )?;
            }
            V2Step::Let(let_step) => {
                // Let step doesn't change pipe value, just adds bindings to context
                current_ctx = eval_v2_let_step(
                    let_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                )?;
            }
            V2Step::If(if_step) => {
                current = eval_v2_if_step(
                    if_step,
                    current,
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                )?;
            }
            V2Step::Map(map_step) => {
                current = eval_v2_map_step(
                    map_step,
                    current,
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                )?;
            }
            V2Step::Ref(v2_ref) => {
                // Reference step evaluates the reference and returns its value
                current = eval_v2_ref(v2_ref, record, context, out, &step_path, &current_ctx)?;
            }
        }
    }

    Ok(current)
}

/// Evaluate a v2 let step - binds variables to context without changing pipe value
pub fn eval_v2_let_step<'a>(
    let_step: &V2LetStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<V2EvalContext<'a>, TransformError> {
    let mut new_ctx = ctx.clone().with_pipe_value(pipe_value);

    for (name, expr) in &let_step.bindings {
        let binding_path = format!("{}.{}", path, name);
        let value = eval_v2_expr(expr, record, context, out, &binding_path, &new_ctx)?;
        new_ctx = new_ctx.with_let_binding(name.clone(), value);
    }

    Ok(new_ctx)
}

/// Evaluate a v2 if step - conditional branching
pub fn eval_v2_if_step<'a>(
    if_step: &V2IfStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Create context with current pipe value for condition evaluation
    let cond_ctx = ctx.clone().with_pipe_value(pipe_value.clone());

    // Evaluate condition
    let cond_path = format!("{}.cond", path);
    let cond_result =
        eval_v2_condition(&if_step.cond, record, context, out, &cond_path, &cond_ctx)?;

    if cond_result {
        // Execute then branch
        let then_path = format!("{}.then", path);
        eval_v2_pipe(
            &if_step.then_branch,
            record,
            context,
            out,
            &then_path,
            &cond_ctx,
        )
    } else if let Some(ref else_branch) = if_step.else_branch {
        // Execute else branch
        let else_path = format!("{}.else", path);
        eval_v2_pipe(else_branch, record, context, out, &else_path, &cond_ctx)
    } else {
        // No else branch, return pipe value unchanged
        Ok(pipe_value)
    }
}

/// Evaluate a v2 map step - iterates over arrays
pub fn eval_v2_map_step<'a>(
    map_step: &V2MapStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Get the array to iterate over
    let arr = match &pipe_value {
        EvalValue::Missing => {
            return Ok(EvalValue::Missing);
        }
        EvalValue::Value(JsonValue::Array(arr)) => arr,
        EvalValue::Value(other) => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                format!("map step requires array, got {:?}", other),
            )
            .with_path(path));
        }
    };

    // Map over each element
    let mut results = Vec::with_capacity(arr.len());
    for (index, item_value) in arr.iter().enumerate() {
        let item_path = format!("{}[{}]", path, index);

        // Create context with item scope
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item_value.clone()))
            .with_item(EvalItem {
                value: item_value,
                index,
            });

        // Apply all steps to this item
        let mut current = EvalValue::Value(item_value.clone());
        let mut step_ctx = item_ctx.clone(); // Declare outside loop to preserve let bindings

        for (step_idx, step) in map_step.steps.iter().enumerate() {
            let step_path = format!("{}.step[{}]", item_path, step_idx);
            step_ctx = step_ctx.clone().with_pipe_value(current.clone());

            match step {
                V2Step::Op(op_step) => {
                    current = eval_v2_op_step(
                        op_step, current, record, context, out, &step_path, &step_ctx,
                    )?;
                }
                V2Step::Let(let_step) => {
                    // Let in map context - evaluate and update context to preserve bindings
                    step_ctx = eval_v2_let_step(
                        let_step,
                        current.clone(),
                        record,
                        context,
                        out,
                        &step_path,
                        &step_ctx,
                    )?;
                    // Let doesn't change pipe value
                    current = step_ctx.get_pipe_value().cloned().unwrap_or(current);
                }
                V2Step::If(if_step) => {
                    current = eval_v2_if_step(
                        if_step, current, record, context, out, &step_path, &step_ctx,
                    )?;
                }
                V2Step::Map(nested_map) => {
                    current = eval_v2_map_step(
                        nested_map, current, record, context, out, &step_path, &step_ctx,
                    )?;
                }
                V2Step::Ref(v2_ref) => {
                    // Reference step evaluates the reference and returns its value
                    current = eval_v2_ref(v2_ref, record, context, out, &step_path, &step_ctx)?;
                }
            };
        }

        // Only add non-missing values to results
        if let EvalValue::Value(v) = current {
            results.push(v);
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

/// Evaluate a v2 expression
pub fn eval_v2_expr<'a>(
    expr: &V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    if let Some(value) = ctx.precomputed_arg_for_path(path) {
        return Ok(value);
    }
    match expr {
        V2Expr::Pipe(pipe) => eval_v2_pipe(pipe, record, context, out, path, ctx),
        V2Expr::V1Fallback(_) => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "v1 fallback not yet implemented",
        )
        .with_path(path)),
    }
}

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

fn value_as_string(value: &JsonValue, path: &str) -> Result<String, TransformError> {
    match value {
        JsonValue::String(value) => Ok(value.clone()),
        _ => Err(
            TransformError::new(TransformErrorKind::ExprError, "value must be a string")
                .with_path(path),
        ),
    }
}

fn value_to_number(value: &JsonValue, path: &str, message: &str) -> Result<f64, TransformError> {
    match value {
        JsonValue::Number(n) => n.as_f64().filter(|f| f.is_finite()).ok_or_else(|| {
            TransformError::new(TransformErrorKind::ExprError, message).with_path(path)
        }),
        JsonValue::String(s) => s
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .ok_or_else(|| {
                TransformError::new(TransformErrorKind::ExprError, message).with_path(path)
            }),
        _ => Err(TransformError::new(TransformErrorKind::ExprError, message).with_path(path)),
    }
}

fn compare_eq_v1(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
) -> Result<bool, TransformError> {
    if left.is_null() || right.is_null() {
        return Ok(left.is_null() && right.is_null());
    }

    let left_value = value_to_string(left, left_path)?;
    let right_value = value_to_string(right, right_path)?;
    Ok(left_value == right_value)
}

fn compare_numbers_v1<F>(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
    compare: F,
) -> Result<bool, TransformError>
where
    F: FnOnce(f64, f64) -> bool,
{
    let left_value = value_to_number(left, left_path, "comparison operand must be a number")?;
    let right_value = value_to_number(right, right_path, "comparison operand must be a number")?;
    Ok(compare(left_value, right_value))
}

fn match_regex_v1(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
) -> Result<bool, TransformError> {
    let value = value_as_string(left, left_path)?;
    let pattern = value_as_string(right, right_path)?;
    let regex = regex::Regex::new(&pattern).map_err(|e| {
        TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid regex pattern: {}", e),
        )
        .with_path(right_path)
    })?;
    Ok(regex.is_match(&value))
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

fn eval_v2_predicate_expr<'a>(
    expr: &V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<bool, TransformError> {
    match eval_v2_expr(expr, record, context, out, path, ctx)? {
        EvalValue::Missing => Ok(false),
        EvalValue::Value(value) => {
            if value.is_null() {
                return Ok(false);
            }
            value_as_bool(&value, path)
        }
    }
}

fn eval_v2_key_expr_string<'a>(
    expr: &V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<String, TransformError> {
    let value = match eval_v2_expr(expr, record, context, out, path, ctx)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(path));
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path));
    }
    value_to_string(&value, path)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortKeyKind {
    Number,
    String,
    Bool,
}

#[derive(Clone)]
enum SortKey {
    Number(f64),
    String(String),
    Bool(bool),
}

impl SortKey {
    fn kind(&self) -> SortKeyKind {
        match self {
            SortKey::Number(_) => SortKeyKind::Number,
            SortKey::String(_) => SortKeyKind::String,
            SortKey::Bool(_) => SortKeyKind::Bool,
        }
    }
}

fn compare_sort_keys(left: &SortKey, right: &SortKey) -> std::cmp::Ordering {
    match (left, right) {
        (SortKey::Number(l), SortKey::Number(r)) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (SortKey::String(l), SortKey::String(r)) => l.cmp(r),
        (SortKey::Bool(l), SortKey::Bool(r)) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    }
}

fn eval_v2_sort_key<'a>(
    expr: &V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<SortKey, TransformError> {
    let value = match eval_v2_expr(expr, record, context, out, path, ctx)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(path));
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path));
    }

    match value {
        JsonValue::Number(number) => {
            let value = number
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| {
                    TransformError::new(
                        TransformErrorKind::ExprError,
                        "sort_by key must be a finite number",
                    )
                    .with_path(path)
                })?;
            Ok(SortKey::Number(value))
        }
        JsonValue::String(value) => Ok(SortKey::String(value)),
        JsonValue::Bool(value) => Ok(SortKey::Bool(value)),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "sort_by key must be string/number/bool",
        )
        .with_path(path)),
    }
}

fn eval_v2_array_from_eval_value(
    value: EvalValue,
    path: &str,
) -> Result<Vec<JsonValue>, TransformError> {
    match value {
        EvalValue::Missing => Ok(Vec::new()),
        EvalValue::Value(value) => {
            if value.is_null() {
                Ok(Vec::new())
            } else if let JsonValue::Array(items) = value {
                Ok(items)
            } else {
                Err(
                    TransformError::new(TransformErrorKind::ExprError, "expr arg must be an array")
                        .with_path(path),
                )
            }
        }
    }
}
fn v2_eval_to_v1_eval(value: &EvalValue) -> V1EvalValue {
    match value {
        EvalValue::Missing => V1EvalValue::Missing,
        EvalValue::Value(v) => V1EvalValue::Value(v.clone()),
    }
}

fn v1_eval_to_v2_eval(value: V1EvalValue) -> EvalValue {
    match value {
        V1EvalValue::Missing => EvalValue::Missing,
        V1EvalValue::Value(v) => EvalValue::Value(v),
    }
}

fn map_v2_op_name(op: &str) -> &str {
    match op {
        "add" => "+",
        "subtract" => "-",
        "multiply" => "*",
        "divide" => "/",
        _ => op,
    }
}

fn eval_v2_op_with_v1_fallback<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    let mut v1_locals_map: HashMap<String, V1EvalValue> = ctx
        .let_bindings()
        .map(|(k, v)| (k.clone(), v2_eval_to_v1_eval(v)))
        .collect();
    let mut arg_refs = Vec::with_capacity(op_step.args.len());
    for (index, arg) in op_step.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", path, index);
        let value = eval_v2_expr(arg, record, context, out, &arg_path, ctx)?;
        let mut key = format!("__v2_arg{}", index);
        if v1_locals_map.contains_key(&key) {
            let mut suffix = 1usize;
            while v1_locals_map.contains_key(&format!("{}{}", key, suffix)) {
                suffix += 1;
            }
            key = format!("{}{}", key, suffix);
        }
        v1_locals_map.insert(key.clone(), v2_eval_to_v1_eval(&value));
        arg_refs.push(Expr::Ref(ExprRef {
            ref_path: format!("local.{}", key),
        }));
    }

    let expr_op = ExprOp {
        op: map_v2_op_name(&op_step.op).to_string(),
        args: arg_refs,
    };

    let v1_pipe = v2_eval_to_v1_eval(&pipe_value);
    let v1_item = ctx.get_item().map(|item| V1EvalItem {
        value: item.value,
        index: item.index,
    });
    let v1_locals = V1EvalLocals {
        item: v1_item,
        acc: ctx.get_acc(),
        pipe: Some(&v1_pipe),
        locals: Some(&v1_locals_map),
        precomputed_op_args: None,
    };

    let result = eval_v1_op(
        &expr_op,
        record,
        context,
        out,
        path,
        Some(&v1_pipe),
        Some(&v1_locals),
    )?;

    Ok(v1_eval_to_v2_eval(result))
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

fn cast_to_int(value: &JsonValue, path: &str) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(JsonValue::Number(i.into()))
            } else if let Some(f) = n.as_f64() {
                if (f.fract()).abs() < f64::EPSILON {
                    Ok(JsonValue::Number((f as i64).into()))
                } else {
                    Err(type_cast_error("int", path))
                }
            } else {
                Err(type_cast_error("int", path))
            }
        }
        JsonValue::String(s) => s
            .parse::<i64>()
            .map(|i| JsonValue::Number(i.into()))
            .map_err(|_| type_cast_error("int", path)),
        _ => Err(type_cast_error("int", path)),
    }
}

fn cast_to_float(value: &JsonValue, path: &str) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Number(n) => n
            .as_f64()
            .ok_or_else(|| type_cast_error("float", path))
            .and_then(|f| {
                serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .ok_or_else(|| type_cast_error("float", path))
            }),
        JsonValue::String(s) => s
            .parse::<f64>()
            .map_err(|_| type_cast_error("float", path))
            .and_then(|f| {
                serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .ok_or_else(|| type_cast_error("float", path))
            }),
        _ => Err(type_cast_error("float", path)),
    }
}

fn cast_to_bool(value: &JsonValue, path: &str) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Bool(b) => Ok(JsonValue::Bool(*b)),
        JsonValue::String(s) => match s.to_lowercase().as_str() {
            "true" => Ok(JsonValue::Bool(true)),
            "false" => Ok(JsonValue::Bool(false)),
            _ => Err(type_cast_error("bool", path)),
        },
        _ => Err(type_cast_error("bool", path)),
    }
}

fn type_cast_error(type_name: &str, path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::ExprError,
        format!("failed to cast to {}", type_name),
    )
    .with_path(path)
}

fn eval_type_cast(op: &str, value: &EvalValue, path: &str) -> Result<EvalValue, TransformError> {
    match value {
        EvalValue::Missing => Ok(EvalValue::Missing),
        EvalValue::Value(v) => {
            let casted = match op {
                "string" => JsonValue::String(value_to_string(v, path)?),
                "int" => cast_to_int(v, path)?,
                "float" => cast_to_float(v, path)?,
                "bool" => cast_to_bool(v, path)?,
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "unknown cast op",
                    )
                    .with_path(path));
                }
            };
            Ok(EvalValue::Value(casted))
        }
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
        "map" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "map requires exactly one argument",
                )
                .with_path(path));
            }
            let array = match pipe_value {
                EvalValue::Missing => {
                    return Ok(EvalValue::Missing);
                }
                EvalValue::Value(JsonValue::Array(items)) => items,
                EvalValue::Value(other) => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        format!("expr arg must be an array, got {:?}", other),
                    )
                    .with_path(path));
                }
            };
            let arg_path = format!("{}.args[0]", path);
            let mut results = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index });
                let value =
                    eval_v2_expr(&op_step.args[0], record, context, out, &arg_path, &item_ctx)?;
                if let EvalValue::Value(value) = value {
                    results.push(value);
                }
            }
            Ok(EvalValue::Value(JsonValue::Array(results)))
        }
        "filter" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "filter requires exactly one argument",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index });
                if eval_v2_predicate_expr(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                )? {
                    results.push(item.clone());
                }
            }
            Ok(EvalValue::Value(JsonValue::Array(results)))
        }
        "flat_map" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "flat_map requires exactly one argument",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index });
                let value = eval_v2_expr_or_null(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                )?;
                match value {
                    JsonValue::Array(items) => results.extend(items),
                    value => results.push(value),
                }
            }
            Ok(EvalValue::Value(JsonValue::Array(results)))
        }
        "group_by" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "group_by requires exactly one argument",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = serde_json::Map::new();
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index });
                let key = eval_v2_key_expr_string(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                )?;
                let entry = results
                    .entry(key)
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                if let JsonValue::Array(items) = entry {
                    items.push(item.clone());
                }
            }
            Ok(EvalValue::Value(JsonValue::Object(results)))
        }
        "key_by" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "key_by requires exactly one argument",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = serde_json::Map::new();
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index });
                let key = eval_v2_key_expr_string(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                )?;
                results.insert(key, item.clone());
            }
            Ok(EvalValue::Value(JsonValue::Object(results)))
        }
        "partition" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "partition requires exactly one argument",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut matched = Vec::new();
            let mut unmatched = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index });
                if eval_v2_predicate_expr(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                )? {
                    matched.push(item.clone());
                } else {
                    unmatched.push(item.clone());
                }
            }
            Ok(EvalValue::Value(JsonValue::Array(vec![
                JsonValue::Array(matched),
                JsonValue::Array(unmatched),
            ])))
        }
        "distinct_by" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "distinct_by requires exactly one argument",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = Vec::new();
            let mut seen = HashSet::new();
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index });
                let key = eval_v2_key_expr_string(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                )?;
                if seen.insert(key) {
                    results.push(item.clone());
                }
            }
            Ok(EvalValue::Value(JsonValue::Array(results)))
        }
        "sort_by" => {
            if !(1..=2).contains(&op_step.args.len()) {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "sort_by requires one or two arguments",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            if array.is_empty() {
                return Ok(EvalValue::Value(JsonValue::Array(Vec::new())));
            }
            let expr_path = format!("{}.args[0]", path);
            let order = if op_step.args.len() == 2 {
                let order_path = format!("{}.args[1]", path);
                let order_value = eval_v2_expr(
                    &op_step.args[1],
                    record,
                    context,
                    out,
                    &order_path,
                    &step_ctx,
                )?;
                let order = match order_value {
                    EvalValue::Missing => return Ok(EvalValue::Missing),
                    EvalValue::Value(value) => value_to_string(&value, &order_path)?,
                };
                if order != "asc" && order != "desc" {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "order must be asc or desc",
                    )
                    .with_path(order_path));
                }
                order
            } else {
                "asc".to_string()
            };

            struct SortItem {
                key: SortKey,
                index: usize,
                value: JsonValue,
            }

            let mut items = Vec::with_capacity(array.len());
            let mut key_kind: Option<SortKeyKind> = None;
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index });
                let key = eval_v2_sort_key(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                )?;
                let kind = key.kind();
                if let Some(existing) = key_kind {
                    if existing != kind {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "sort_by keys must be all the same type",
                        )
                        .with_path(expr_path));
                    }
                } else {
                    key_kind = Some(kind);
                }
                items.push(SortItem {
                    key,
                    index,
                    value: item.clone(),
                });
            }

            items.sort_by(|left, right| {
                let mut ordering = compare_sort_keys(&left.key, &right.key);
                if order == "desc" {
                    ordering = ordering.reverse();
                }
                if ordering == std::cmp::Ordering::Equal {
                    left.index.cmp(&right.index)
                } else {
                    ordering
                }
            });

            let results = items.into_iter().map(|item| item.value).collect::<Vec<_>>();
            Ok(EvalValue::Value(JsonValue::Array(results)))
        }
        "find" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "find requires exactly one argument",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            let arg_path = format!("{}.args[0]", path);
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index });
                if eval_v2_predicate_expr(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                )? {
                    return Ok(EvalValue::Value(item.clone()));
                }
            }
            Ok(EvalValue::Value(JsonValue::Null))
        }
        "find_index" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "find_index requires exactly one argument",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            let arg_path = format!("{}.args[0]", path);
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index });
                if eval_v2_predicate_expr(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                )? {
                    return Ok(EvalValue::Value(JsonValue::Number((index as i64).into())));
                }
            }
            Ok(EvalValue::Value(JsonValue::Number((-1).into())))
        }
        "reduce" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "reduce requires exactly one argument",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            if array.is_empty() {
                return Ok(EvalValue::Value(JsonValue::Null));
            }
            let expr_path = format!("{}.args[0]", path);
            let mut acc = array[0].clone();
            for (index, item) in array.iter().enumerate().skip(1) {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index })
                    .with_acc(&acc);
                let value = eval_v2_expr_or_null(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                )?;
                acc = value;
            }
            Ok(EvalValue::Value(acc))
        }
        "fold" => {
            if op_step.args.len() != 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "fold requires exactly two arguments",
                )
                .with_path(path));
            }
            let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
            let init_path = format!("{}.args[0]", path);
            let initial = match eval_v2_expr(
                &op_step.args[0],
                record,
                context,
                out,
                &init_path,
                &step_ctx,
            )? {
                EvalValue::Missing => return Ok(EvalValue::Missing),
                EvalValue::Value(value) => value,
            };
            let expr_path = format!("{}.args[1]", path);
            let mut acc = initial;
            for (index, item) in array.iter().enumerate() {
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(item.clone()))
                    .with_item(EvalItem { value: item, index })
                    .with_acc(&acc);
                let value = eval_v2_expr_or_null(
                    &op_step.args[1],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                )?;
                acc = value;
            }
            Ok(EvalValue::Value(acc))
        }
        "zip_with" => {
            if op_step.args.len() < 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "zip_with requires at least two arguments",
                )
                .with_path(path));
            }
            let mut arrays = Vec::new();
            arrays.push(eval_v2_array_from_eval_value(pipe_value.clone(), path)?);
            for (index, arg) in op_step.args.iter().enumerate().take(op_step.args.len() - 1) {
                let arg_path = format!("{}.args[{}]", path, index);
                let value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                arrays.push(eval_v2_array_from_eval_value(value, &arg_path)?);
            }

            let min_len = arrays.iter().map(|items| items.len()).min().unwrap_or(0);
            let expr_index = op_step.args.len() - 1;
            let expr_path = format!("{}.args[{}]", path, expr_index);
            let expr = &op_step.args[expr_index];
            let mut results = Vec::with_capacity(min_len);
            for row_index in 0..min_len {
                let mut row = Vec::with_capacity(arrays.len());
                for array in &arrays {
                    row.push(array[row_index].clone());
                }
                let row_value = JsonValue::Array(row);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(EvalValue::Value(row_value.clone()))
                    .with_item(EvalItem {
                        value: &row_value,
                        index: row_index,
                    });
                let value =
                    eval_v2_expr_or_null(expr, record, context, out, &expr_path, &item_ctx)?;
                results.push(value);
            }
            Ok(EvalValue::Value(JsonValue::Array(results)))
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
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr.args must contain exactly one item",
                )
                .with_path(format!("{}.args", path)));
            }
            let left = match pipe_value {
                EvalValue::Missing => JsonValue::Null,
                EvalValue::Value(value) => value,
            };
            let right_path = format!("{}.args[0]", path);
            let right = eval_v2_expr_or_null(
                &op_step.args[0],
                record,
                context,
                out,
                &right_path,
                &step_ctx,
            )?;
            let left_path = path.to_string();
            let op = match op_step.op.as_str() {
                "eq" => "==",
                "ne" => "!=",
                "lt" => "<",
                "lte" => "<=",
                "gt" => ">",
                "gte" => ">=",
                "match" => "~=",
                other => other,
            };
            let result = match op {
                "==" => compare_eq_v1(&left, &right, &left_path, &right_path)?,
                "!=" => !compare_eq_v1(&left, &right, &left_path, &right_path)?,
                "<" => compare_numbers_v1(&left, &right, &left_path, &right_path, |l, r| l < r)?,
                "<=" => compare_numbers_v1(&left, &right, &left_path, &right_path, |l, r| l <= r)?,
                ">" => compare_numbers_v1(&left, &right, &left_path, &right_path, |l, r| l > r)?,
                ">=" => compare_numbers_v1(&left, &right, &left_path, &right_path, |l, r| l >= r)?,
                "~=" => match_regex_v1(&left, &right, &left_path, &right_path)?,
                _ => false,
            };
            Ok(EvalValue::Value(JsonValue::Bool(result)))
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

        // Lookup operations - v2 keyword format: lookup_first: {from: ..., match: [...], get: ...}
        // For v2, lookup args are parsed from V2OpStep with special handling
        // Explicit from:
        // args[0] = from (array to search in)
        // args[1] = match key (field name in array items to match)
        // args[2] = match value (value to match against)
        // args[3] = get (optional - field to extract from matched item)
        // Implicit from (pipe value):
        // args[0] = match key
        // args[1] = match value
        // args[2] = get (optional)
        "lookup_first" => {
            if op_step.args.len() < 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "lookup_first requires at least 2 arguments: match_key, match_value",
                )
                .with_path(path));
            }

            let args = &op_step.args;
            let from_path = format!("{}.from", path);
            let match_key_path = format!("{}.match_key", path);
            let get_path = format!("{}.get", path);

            let (from_value, match_key_value, match_value, get_field) = match args.len() {
                0 | 1 => unreachable!("guarded above"),
                2 => {
                    let match_key_value = eval_v2_expr(
                        &args[0],
                        record,
                        context,
                        out,
                        &format!("{}.args[0]", path),
                        &step_ctx,
                    )?;
                    let match_value = eval_v2_expr(
                        &args[1],
                        record,
                        context,
                        out,
                        &format!("{}.args[1]", path),
                        &step_ctx,
                    )?;
                    (pipe_value.clone(), match_key_value, match_value, None)
                }
                3 => {
                    if matches!(pipe_value, EvalValue::Missing) {
                        let first_value = eval_v2_expr(
                            &args[0],
                            record,
                            context,
                            out,
                            &format!("{}.args[0]", path),
                            &step_ctx,
                        )?;
                        let use_explicit_from =
                            matches!(first_value, EvalValue::Value(JsonValue::Array(_)));
                        if !use_explicit_from {
                            return Ok(EvalValue::Missing);
                        }
                        let match_key_value = eval_v2_expr(
                            &args[1],
                            record,
                            context,
                            out,
                            &format!("{}.args[1]", path),
                            &step_ctx,
                        )?;
                        let match_value = eval_v2_expr(
                            &args[2],
                            record,
                            context,
                            out,
                            &format!("{}.args[2]", path),
                            &step_ctx,
                        )?;
                        (first_value, match_key_value, match_value, None)
                    } else {
                        let first_value = eval_v2_expr(
                            &args[0],
                            record,
                            context,
                            out,
                            &format!("{}.args[0]", path),
                            &step_ctx,
                        )?;
                        let use_explicit_from = matches!(
                            first_value,
                            EvalValue::Value(JsonValue::Array(_)) | EvalValue::Missing
                        );
                        if use_explicit_from {
                            let match_key_value = eval_v2_expr(
                                &args[1],
                                record,
                                context,
                                out,
                                &format!("{}.args[1]", path),
                                &step_ctx,
                            )?;
                            let match_value = eval_v2_expr(
                                &args[2],
                                record,
                                context,
                                out,
                                &format!("{}.args[2]", path),
                                &step_ctx,
                            )?;
                            (first_value, match_key_value, match_value, None)
                        } else {
                            let match_value = eval_v2_expr(
                                &args[1],
                                record,
                                context,
                                out,
                                &format!("{}.args[1]", path),
                                &step_ctx,
                            )?;
                            let get_value = eval_v2_expr(
                                &args[2],
                                record,
                                context,
                                out,
                                &format!("{}.args[2]", path),
                                &step_ctx,
                            )?;
                            let get_field = Some(eval_value_as_string(&get_value, &get_path)?);
                            (pipe_value.clone(), first_value, match_value, get_field)
                        }
                    }
                }
                _ => {
                    let from_value = eval_v2_expr(
                        &args[0],
                        record,
                        context,
                        out,
                        &format!("{}.args[0]", path),
                        &step_ctx,
                    )?;
                    let match_key_value = eval_v2_expr(
                        &args[1],
                        record,
                        context,
                        out,
                        &format!("{}.args[1]", path),
                        &step_ctx,
                    )?;
                    let match_value = eval_v2_expr(
                        &args[2],
                        record,
                        context,
                        out,
                        &format!("{}.args[2]", path),
                        &step_ctx,
                    )?;
                    let get_value = eval_v2_expr(
                        &args[3],
                        record,
                        context,
                        out,
                        &format!("{}.args[3]", path),
                        &step_ctx,
                    )?;
                    let get_field = Some(eval_value_as_string(&get_value, &get_path)?);
                    (from_value, match_key_value, match_value, get_field)
                }
            };

            // Evaluate 'from' - the array to search in
            let arr = match &from_value {
                EvalValue::Value(JsonValue::Array(arr)) => arr,
                EvalValue::Missing => return Ok(EvalValue::Missing),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "lookup_first 'from' must be an array",
                    )
                    .with_path(&from_path));
                }
            };

            // Get match key as string
            let match_key = eval_value_as_string(&match_key_value, &match_key_path)?;
            if matches!(match_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }

            // Search for first matching item
            for item in arr {
                if let JsonValue::Object(obj) = item {
                    if let Some(field_val) = obj.get(&match_key) {
                        let item_val = EvalValue::Value(field_val.clone());
                        if compare_values_eq(&item_val, &match_value) {
                            // Found a match
                            if let Some(ref get_key) = get_field {
                                // Return specific field from matched item
                                return match obj.get(get_key) {
                                    Some(v) => Ok(EvalValue::Value(v.clone())),
                                    None => Ok(EvalValue::Missing),
                                };
                            } else {
                                // Return entire matched item
                                return Ok(EvalValue::Value(item.clone()));
                            }
                        }
                    }
                }
            }

            Ok(EvalValue::Missing)
        }

        "lookup" => {
            if op_step.args.len() < 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "lookup requires at least 2 arguments: match_key, match_value",
                )
                .with_path(path));
            }

            let args = &op_step.args;
            let from_path = format!("{}.from", path);
            let match_key_path = format!("{}.match_key", path);
            let get_path = format!("{}.get", path);

            let (from_value, match_key_value, match_value, get_field) = match args.len() {
                0 | 1 => unreachable!("guarded above"),
                2 => {
                    let match_key_value = eval_v2_expr(
                        &args[0],
                        record,
                        context,
                        out,
                        &format!("{}.args[0]", path),
                        &step_ctx,
                    )?;
                    let match_value = eval_v2_expr(
                        &args[1],
                        record,
                        context,
                        out,
                        &format!("{}.args[1]", path),
                        &step_ctx,
                    )?;
                    (pipe_value.clone(), match_key_value, match_value, None)
                }
                3 => {
                    if matches!(pipe_value, EvalValue::Missing) {
                        let first_value = eval_v2_expr(
                            &args[0],
                            record,
                            context,
                            out,
                            &format!("{}.args[0]", path),
                            &step_ctx,
                        )?;
                        let use_explicit_from =
                            matches!(first_value, EvalValue::Value(JsonValue::Array(_)));
                        if !use_explicit_from {
                            return Ok(EvalValue::Missing);
                        }
                        let match_key_value = eval_v2_expr(
                            &args[1],
                            record,
                            context,
                            out,
                            &format!("{}.args[1]", path),
                            &step_ctx,
                        )?;
                        let match_value = eval_v2_expr(
                            &args[2],
                            record,
                            context,
                            out,
                            &format!("{}.args[2]", path),
                            &step_ctx,
                        )?;
                        (first_value, match_key_value, match_value, None)
                    } else {
                        let first_value = eval_v2_expr(
                            &args[0],
                            record,
                            context,
                            out,
                            &format!("{}.args[0]", path),
                            &step_ctx,
                        )?;
                        let use_explicit_from = matches!(
                            first_value,
                            EvalValue::Value(JsonValue::Array(_)) | EvalValue::Missing
                        );
                        if use_explicit_from {
                            let match_key_value = eval_v2_expr(
                                &args[1],
                                record,
                                context,
                                out,
                                &format!("{}.args[1]", path),
                                &step_ctx,
                            )?;
                            let match_value = eval_v2_expr(
                                &args[2],
                                record,
                                context,
                                out,
                                &format!("{}.args[2]", path),
                                &step_ctx,
                            )?;
                            (first_value, match_key_value, match_value, None)
                        } else {
                            let match_value = eval_v2_expr(
                                &args[1],
                                record,
                                context,
                                out,
                                &format!("{}.args[1]", path),
                                &step_ctx,
                            )?;
                            let get_value = eval_v2_expr(
                                &args[2],
                                record,
                                context,
                                out,
                                &format!("{}.args[2]", path),
                                &step_ctx,
                            )?;
                            let get_field = Some(eval_value_as_string(&get_value, &get_path)?);
                            (pipe_value.clone(), first_value, match_value, get_field)
                        }
                    }
                }
                _ => {
                    let from_value = eval_v2_expr(
                        &args[0],
                        record,
                        context,
                        out,
                        &format!("{}.args[0]", path),
                        &step_ctx,
                    )?;
                    let match_key_value = eval_v2_expr(
                        &args[1],
                        record,
                        context,
                        out,
                        &format!("{}.args[1]", path),
                        &step_ctx,
                    )?;
                    let match_value = eval_v2_expr(
                        &args[2],
                        record,
                        context,
                        out,
                        &format!("{}.args[2]", path),
                        &step_ctx,
                    )?;
                    let get_value = eval_v2_expr(
                        &args[3],
                        record,
                        context,
                        out,
                        &format!("{}.args[3]", path),
                        &step_ctx,
                    )?;
                    let get_field = Some(eval_value_as_string(&get_value, &get_path)?);
                    (from_value, match_key_value, match_value, get_field)
                }
            };

            // Evaluate 'from' - the array to search in
            let arr = match &from_value {
                EvalValue::Value(JsonValue::Array(arr)) => arr,
                EvalValue::Missing => return Ok(EvalValue::Missing),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "lookup 'from' must be an array",
                    )
                    .with_path(&from_path));
                }
            };

            // Get match key as string
            let match_key = eval_value_as_string(&match_key_value, &match_key_path)?;
            if matches!(match_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }

            // Search for ALL matching items
            let mut results = Vec::new();
            for item in arr {
                if let JsonValue::Object(obj) = item {
                    if let Some(field_val) = obj.get(&match_key) {
                        let item_val = EvalValue::Value(field_val.clone());
                        if compare_values_eq(&item_val, &match_value) {
                            // Found a match
                            if let Some(ref get_key) = get_field {
                                // Add specific field from matched item
                                if let Some(v) = obj.get(get_key) {
                                    results.push(v.clone());
                                }
                            } else {
                                // Add entire matched item
                                results.push(item.clone());
                            }
                        }
                    }
                }
            }

            Ok(EvalValue::Value(JsonValue::Array(results)))
        }

        // Default case - fall back to v1 op evaluation
        _ => {
            eval_v2_op_with_v1_fallback(op_step, pipe_value, record, context, out, path, &step_ctx)
        }
    }
}
