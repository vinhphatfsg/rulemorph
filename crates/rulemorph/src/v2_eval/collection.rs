use serde_json::Value as JsonValue;

use super::{
    EvalItem, EvalValue, V2EvalContext, eval_v2_expr, eval_v2_expr_or_null, value_to_string,
};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::{V2Expr, V2OpStep};

mod keyed;
mod predicate;
mod reduce_fold;
mod sequence;
mod sort;

use keyed::eval_keyed_collection;
use predicate::{eval_filter, eval_find, eval_find_index, eval_partition};
use reduce_fold::{eval_fold, eval_reduce};
use sequence::{eval_flat_map, eval_map, eval_zip_with};
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
        "map" => eval_map(op_step, pipe_value, record, context, out, path, ctx),
        "filter" => eval_filter(op_step, pipe_value, record, context, out, path, ctx),
        "flat_map" => eval_flat_map(op_step, pipe_value, record, context, out, path, ctx),
        "group_by" | "key_by" | "distinct_by" => {
            eval_keyed_collection(op_step, pipe_value, record, context, out, path, ctx)
        }
        "partition" => eval_partition(op_step, pipe_value, record, context, out, path, ctx),
        "sort_by" => eval_sort_by(op_step, pipe_value, record, context, out, path, ctx),
        "find" => eval_find(op_step, pipe_value, record, context, out, path, ctx),
        "find_index" => eval_find_index(op_step, pipe_value, record, context, out, path, ctx),
        "reduce" => eval_reduce(op_step, pipe_value, record, context, out, path, ctx),
        "fold" => eval_fold(op_step, pipe_value, record, context, out, path, ctx),
        "zip_with" => eval_zip_with(op_step, pipe_value, record, context, out, path, ctx),
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

pub(super) fn eval_v2_key_expr_string<'a>(
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

pub(super) fn eval_v2_array_from_eval_value(
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
