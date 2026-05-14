use serde_json::Value as JsonValue;

use super::{EvalValue, value_to_string};
use crate::error::{TransformError, TransformErrorKind};

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

pub(super) fn eval_type_cast(
    op: &str,
    value: &EvalValue,
    path: &str,
) -> Result<EvalValue, TransformError> {
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
