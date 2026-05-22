use serde_json::Value as JsonValue;

use super::super::{EvalValue, V2EvalContext, eval_v2_expr, value_as_bool};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::V2OpStep;

pub(super) fn eval_first_last_op(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    path: &str,
) -> Result<EvalValue, TransformError> {
    match &pipe_value {
        EvalValue::Missing => Ok(EvalValue::Missing),
        EvalValue::Value(JsonValue::Array(arr)) => {
            let value = if op_step.op == "first" {
                arr.first()
            } else {
                arr.last()
            };
            if let Some(value) = value {
                Ok(EvalValue::Value(value.clone()))
            } else {
                Ok(EvalValue::Missing)
            }
        }
        EvalValue::Value(other) => Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("{} requires array, got {:?}", op_step.op, other),
        )
        .with_path(path)),
    }
}

pub(super) fn eval_coalesce_op<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // If pipe value is present and not null, use it
    if let EvalValue::Value(v) = &pipe_value {
        if !v.is_null() {
            return Ok(pipe_value);
        }
    }
    // Otherwise, try args in order
    for (i, arg) in op_step.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", path, i);
        let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, ctx)?;
        if let EvalValue::Value(v) = &arg_value {
            if !v.is_null() {
                return Ok(arg_value);
            }
        }
    }
    Ok(EvalValue::Missing)
}

pub(super) fn eval_logical_op<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
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
        let value = eval_v2_expr(arg, record, context, out, &arg_path, ctx)?;
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

pub(super) fn eval_not_op(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    path: &str,
) -> Result<EvalValue, TransformError> {
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
