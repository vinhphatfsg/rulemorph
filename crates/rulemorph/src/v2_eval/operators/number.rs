use serde_json::Value as JsonValue;

use super::super::{EvalValue, V2EvalContext, eval_v2_expr, eval_value_as_number};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::V2OpStep;

pub(super) fn eval_number_op<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    match op_step.op.as_str() {
        "add" | "+" => {
            if matches!(pipe_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }
            let mut result = eval_value_as_number(&pipe_value, path)?;
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, ctx)?;
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
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, ctx)?;
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
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, ctx)?;
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
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, ctx)?;
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
        _ => unreachable!("number dispatcher only calls numeric operators"),
    }
}
