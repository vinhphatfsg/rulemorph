use serde_json::Value as JsonValue;
use std::collections::HashSet;

use super::{
    EvalItem, EvalValue, V2EvalContext, eval_v2_expr, eval_v2_expr_or_null, value_to_string,
};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::{V2Expr, V2OpStep};

mod predicate;
mod sort;

use predicate::{eval_filter, eval_find, eval_find_index, eval_partition};
use sort::eval_sort_by;

pub(super) fn eval_collection_op<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    match op_step.op.as_str() {
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
                let item_ctx = ctx
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
        "filter" => eval_filter(op_step, pipe_value, record, context, out, path, ctx),
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
                let item_ctx = ctx
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
                let item_ctx = ctx
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
                let item_ctx = ctx
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
        "partition" => eval_partition(op_step, pipe_value, record, context, out, path, ctx),
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
                let item_ctx = ctx
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
        "sort_by" => eval_sort_by(op_step, pipe_value, record, context, out, path, ctx),
        "find" => eval_find(op_step, pipe_value, record, context, out, path, ctx),
        "find_index" => eval_find_index(op_step, pipe_value, record, context, out, path, ctx),
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
                let item_ctx = ctx
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
            let initial =
                match eval_v2_expr(&op_step.args[0], record, context, out, &init_path, ctx)? {
                    EvalValue::Missing => return Ok(EvalValue::Missing),
                    EvalValue::Value(value) => value,
                };
            let expr_path = format!("{}.args[1]", path);
            let mut acc = initial;
            for (index, item) in array.iter().enumerate() {
                let item_ctx = ctx
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
                let value = eval_v2_expr(arg, record, context, out, &arg_path, ctx)?;
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
                let item_ctx = ctx
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
        _ => unreachable!("collection dispatcher only calls collection operators"),
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
