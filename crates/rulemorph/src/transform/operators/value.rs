use super::*;

mod cast;

pub(in crate::transform) use self::cast::cast_value;

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
    if !(2..=36).contains(&base) {
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

pub(in crate::transform) fn value_to_string_optional(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(number_to_string(n)),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

pub(in crate::transform) fn value_matches_string_key(value: &JsonValue, key: &str) -> bool {
    match value {
        JsonValue::String(s) => s == key,
        JsonValue::Number(n) => number_to_string(n) == key,
        JsonValue::Bool(false) => key == "false",
        JsonValue::Bool(true) => key == "true",
        _ => false,
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
