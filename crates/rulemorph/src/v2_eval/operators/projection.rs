use serde_json::Value as JsonValue;

use super::super::{EvalValue, V2EvalContext, eval_v2_expr, eval_v2_op_with_v1_fallback};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::{V2Expr, V2OpStep, V2Pipe, V2Start};

pub(super) fn eval_projection_op<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
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
        let value = match eval_v2_expr(arg, record, context, out, &arg_path, ctx)? {
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
    eval_v2_op_with_v1_fallback(&normalized_op, pipe_value, record, context, out, path, ctx)
}
