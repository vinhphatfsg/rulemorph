use super::*;

pub(in crate::transform) fn value_to_string(
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

pub(super) fn value_as_string(value: &JsonValue, path: &str) -> Result<String, TransformError> {
    match value {
        JsonValue::String(s) => Ok(s.clone()),
        _ => Err(
            TransformError::new(TransformErrorKind::ExprError, "value must be a string")
                .with_path(path),
        ),
    }
}

pub(in crate::transform) fn value_as_bool(
    value: &JsonValue,
    path: &str,
) -> Result<bool, TransformError> {
    match value {
        JsonValue::Bool(flag) => Ok(*flag),
        _ => Err(expr_type_error("value must be a boolean", path)),
    }
}

pub(super) fn value_to_number(
    value: &JsonValue,
    path: &str,
    message: &str,
) -> Result<f64, TransformError> {
    match value {
        JsonValue::Number(n) => n
            .as_f64()
            .filter(|f| f.is_finite())
            .ok_or_else(|| expr_type_error(message, path)),
        JsonValue::String(s) => s
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .ok_or_else(|| expr_type_error(message, path)),
        _ => Err(expr_type_error(message, path)),
    }
}

pub(super) fn value_to_i64(
    value: &JsonValue,
    path: &str,
    message: &str,
) -> Result<i64, TransformError> {
    match value {
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).map_err(|_| expr_type_error(message, path))
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() && (f.fract()).abs() < f64::EPSILON {
                    let value = f as i64;
                    if (value as f64 - f).abs() < f64::EPSILON {
                        Ok(value)
                    } else {
                        Err(expr_type_error(message, path))
                    }
                } else {
                    Err(expr_type_error(message, path))
                }
            } else {
                Err(expr_type_error(message, path))
            }
        }
        JsonValue::String(s) => s.parse::<i64>().map_err(|_| expr_type_error(message, path)),
        _ => Err(expr_type_error(message, path)),
    }
}

pub(super) fn json_number_from_f64(value: f64, path: &str) -> Result<JsonValue, TransformError> {
    if !value.is_finite() {
        return Err(expr_type_error("number result is not finite", path));
    }
    if (value.fract()).abs() < f64::EPSILON {
        let as_i64 = value as i64;
        if (as_i64 as f64 - value).abs() < f64::EPSILON {
            return Ok(JsonValue::Number(as_i64.into()));
        }
    }
    serde_json::Number::from_f64(value)
        .map(JsonValue::Number)
        .ok_or_else(|| expr_type_error("number result is not finite", path))
}

pub(super) fn to_radix_string(value: i64, base: u32, path: &str) -> Result<String, TransformError> {
    let digits = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if base < 2 || base > 36 {
        return Err(expr_type_error("base must be between 2 and 36", path));
    }

    if value == 0 {
        return Ok("0".to_string());
    }

    let is_negative = value < 0;
    let mut n = value
        .checked_abs()
        .ok_or_else(|| expr_type_error("value is out of range for base conversion", path))?
        as u64;

    let mut buf = Vec::new();
    while n > 0 {
        let idx = (n % base as u64) as usize;
        buf.push(digits[idx] as char);
        n /= base as u64;
    }
    if is_negative {
        buf.push('-');
    }
    buf.reverse();
    Ok(buf.iter().collect())
}

pub(super) fn value_to_string_optional(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(number_to_string(n)),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

pub(super) fn expr_type_error(message: &str, path: &str) -> TransformError {
    TransformError::new(TransformErrorKind::ExprError, message).with_path(path)
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

pub(in crate::transform) fn cast_value(
    value: &JsonValue,
    type_name: &str,
    path: &str,
) -> Result<JsonValue, TransformError> {
    match type_name {
        "string" => Ok(JsonValue::String(value_to_string(value, path)?)),
        "int" => cast_to_int(value, path),
        "float" => cast_to_float(value, path),
        "bool" => cast_to_bool(value, path),
        _ => Err(TransformError::new(
            TransformErrorKind::TypeCastFailed,
            "type must be string|int|float|bool",
        )
        .with_path(path)),
    }
}

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
        TransformErrorKind::TypeCastFailed,
        format!("failed to cast to {}", type_name),
    )
    .with_path(path)
}
