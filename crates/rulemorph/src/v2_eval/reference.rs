use serde_json::Value as JsonValue;

use super::{EvalValue, V2EvalContext};
use crate::error::{TransformError, TransformErrorKind};
use crate::path::{get_path, parse_path};
use crate::v2_model::{V2Ref, V2Start};

/// Helper to get value at path string
fn get_path_str(
    value: &JsonValue,
    path_str: &str,
    error_path: &str,
) -> Result<EvalValue, TransformError> {
    let tokens = parse_path(path_str).map_err(|_| {
        TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid path: {}", path_str),
        )
        .with_path(error_path)
    })?;
    match get_path(value, &tokens) {
        Some(v) => Ok(EvalValue::Value(v.clone())),
        None => Ok(EvalValue::Missing),
    }
}

/// Evaluate a v2 reference to get its value
pub fn eval_v2_ref<'a>(
    v2_ref: &V2Ref,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    match v2_ref {
        V2Ref::Input(ref_path) => {
            if ref_path.is_empty() {
                Ok(EvalValue::Value(record.clone()))
            } else {
                get_path_str(record, ref_path, path)
            }
        }
        V2Ref::Context(ref_path) => {
            let ctx_value = match context {
                Some(value) => value,
                None => return Ok(EvalValue::Missing),
            };
            if ref_path.is_empty() {
                Ok(EvalValue::Value(ctx_value.clone()))
            } else {
                get_path_str(ctx_value, ref_path, path)
            }
        }
        V2Ref::Out(ref_path) => {
            if ref_path.is_empty() {
                Ok(EvalValue::Value(out.clone()))
            } else {
                get_path_str(out, ref_path, path)
            }
        }
        V2Ref::Pipe(ref_path) => {
            let value = ctx.get_pipe_value().ok_or_else(|| {
                TransformError::new(TransformErrorKind::ExprError, "$ is not available")
                    .with_path(path)
            })?;
            match value {
                EvalValue::Missing => Ok(EvalValue::Missing),
                EvalValue::Value(value) => get_path_str(value, ref_path, path),
            }
        }
        V2Ref::Item(ref_path) => {
            let item = ctx.get_item().ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "@item is only available in map/filter operations",
                )
                .with_path(path)
            })?;
            if ref_path.is_empty() {
                Ok(EvalValue::Value(item.value.clone()))
            } else if ref_path == "index" {
                Ok(EvalValue::Value(JsonValue::Number(item.index.into())))
            } else if let Some(rest) = ref_path.strip_prefix("value.") {
                get_path_str(item.value, rest, path)
            } else if ref_path == "value" {
                Ok(EvalValue::Value(item.value.clone()))
            } else {
                // Direct path on item value
                get_path_str(item.value, ref_path, path)
            }
        }
        V2Ref::Acc(ref_path) => {
            let acc = ctx.get_acc().ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "@acc is only available in reduce/fold operations",
                )
                .with_path(path)
            })?;
            if ref_path.is_empty() {
                Ok(EvalValue::Value(acc.clone()))
            } else if let Some(rest) = ref_path.strip_prefix("value.") {
                get_path_str(acc, rest, path)
            } else if ref_path == "value" {
                Ok(EvalValue::Value(acc.clone()))
            } else {
                // Direct path on acc value
                get_path_str(acc, ref_path, path)
            }
        }
        V2Ref::Local(var_name) => {
            let value = ctx.resolve_local(var_name).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("undefined variable: @{}", var_name),
                )
                .with_path(path)
            })?;
            Ok(value.clone())
        }
    }
}

/// Evaluate a v2 start value
pub fn eval_v2_start<'a>(
    start: &V2Start,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    match start {
        V2Start::Ref(v2_ref) => eval_v2_ref(v2_ref, record, context, out, path, ctx),
        V2Start::PipeValue | V2Start::ImplicitPipeValue => {
            // If pipe value is not available, return Missing instead of error
            // This allows ops like lookup_first that don't use pipe input to work
            Ok(ctx.get_pipe_value().cloned().unwrap_or(EvalValue::Missing))
        }
        V2Start::Literal(value) => Ok(EvalValue::Value(value.clone())),
        V2Start::V1Expr(_expr) => {
            // V1 expressions would be evaluated using the existing v1 eval logic
            // For now, return an error as this is a fallback case
            Err(TransformError::new(
                TransformErrorKind::ExprError,
                "v1 expression fallback not yet implemented",
            )
            .with_path(path))
        }
    }
}
