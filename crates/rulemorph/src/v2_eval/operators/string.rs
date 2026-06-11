use super::super::{EvalValue, V2EvalContext, eval_v2_expr, eval_value_as_string};
use crate::error::TransformError;
use crate::v2_model::V2OpStep;
use serde_json::Value as JsonValue;

pub(super) fn eval_string_op<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    step_ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    match op_step.op.as_str() {
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
            let mut parts = Vec::new();
            if matches!(pipe_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }
            parts.push(eval_value_as_string(&pipe_value, path)?);
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, step_ctx)?;
                if matches!(arg_value, EvalValue::Missing) {
                    return Ok(EvalValue::Missing);
                }
                parts.push(eval_value_as_string(&arg_value, &arg_path)?);
            }
            Ok(EvalValue::Value(JsonValue::String(parts.join(""))))
        }
        _ => unreachable!("string dispatcher only calls direct string operators"),
    }
}
