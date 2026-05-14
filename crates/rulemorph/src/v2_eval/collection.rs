use serde_json::Value as JsonValue;
use std::collections::HashSet;

use super::{
    EvalItem, EvalValue, V2EvalContext, eval_v2_expr, eval_v2_expr_or_null, value_to_string,
};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::{V2Expr, V2OpStep};

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
                let item_ctx = ctx
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
                let item_ctx = ctx
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
                let item_ctx = ctx
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
                let item_ctx = ctx
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

fn eval_sort_by<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
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
        let order_value = eval_v2_expr(&op_step.args[1], record, context, out, &order_path, ctx)?;
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
        let item_ctx = ctx
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
