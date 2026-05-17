use serde_json::Value as JsonValue;

use super::{EvalValue, V2EvalContext, eval_v2_expr};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::V2Expr;

/// Helper to convert EvalValue to string
pub(in crate::v2_eval) fn eval_value_as_string(
    value: &EvalValue,
    path: &str,
) -> Result<String, TransformError> {
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
pub(in crate::v2_eval) fn eval_value_as_number(
    value: &EvalValue,
    path: &str,
) -> Result<f64, TransformError> {
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

pub(in crate::v2_eval) fn value_as_bool(
    value: &JsonValue,
    path: &str,
) -> Result<bool, TransformError> {
    match value {
        JsonValue::Bool(flag) => Ok(*flag),
        _ => Err(
            TransformError::new(TransformErrorKind::ExprError, "value must be a boolean")
                .with_path(path),
        ),
    }
}

pub(in crate::v2_eval) fn eval_v2_expr_or_null<'a>(
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

pub(in crate::v2_eval) fn value_to_string(
    value: &JsonValue,
    path: &str,
) -> Result<String, TransformError> {
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
