use serde_json::Number as JsonNumber;

use super::expr_error;
use crate::error::TransformError;

pub(in crate::v2_eval::operators::typed_value) fn validate_dynamodb_number(
    raw: &str,
    path: &str,
) -> Result<(), TransformError> {
    let decimal = parse_strict_decimal(raw, false, false, "invalid DynamoDB number", path)?;
    if decimal.significant_digits > 38 {
        return Err(expr_error(
            "DynamoDB number exceeds 38 significant digits",
            path,
        ));
    }
    if let Some(exponent) = decimal.adjusted_exponent.or(decimal.explicit_exponent)
        && !(-130..=125).contains(&exponent)
    {
        return Err(expr_error(
            "DynamoDB number is outside supported exponent range",
            path,
        ));
    }
    Ok(())
}

struct DecimalShape {
    significant_digits: usize,
    adjusted_exponent: Option<i32>,
    explicit_exponent: Option<i32>,
}

fn parse_strict_decimal(
    raw: &str,
    allow_plus: bool,
    count_trailing_zeroes: bool,
    invalid_message: &str,
    path: &str,
) -> Result<DecimalShape, TransformError> {
    let s = raw.trim();
    if s != raw || s.is_empty() || s.eq_ignore_ascii_case("nan") || s.eq_ignore_ascii_case("inf") {
        return Err(expr_error(invalid_message, path));
    }
    let bytes = s.as_bytes();
    let mut i = 0usize;
    if bytes.first().is_some_and(|b| *b == b'-') {
        i = 1;
    } else if bytes.first().is_some_and(|b| *b == b'+') {
        if !allow_plus {
            return Err(expr_error(invalid_message, path));
        }
        i = 1;
    }

    let mut mantissa_digits = 0i32;
    let mut integer_digits = 0i32;
    let mut first_nonzero_digit: Option<i32> = None;
    let mut last_nonzero_digit: Option<i32> = None;
    let integer_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        if bytes[i] != b'0' {
            first_nonzero_digit.get_or_insert(mantissa_digits);
            last_nonzero_digit = Some(mantissa_digits);
        }
        mantissa_digits += 1;
        integer_digits += 1;
        i += 1;
    }
    let integer_count = i - integer_start;
    if i < bytes.len() && bytes[i] == b'.' {
        if integer_count == 0 {
            return Err(expr_error(invalid_message, path));
        }
        i += 1;
        let fraction_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            if bytes[i] != b'0' {
                first_nonzero_digit.get_or_insert(mantissa_digits);
                last_nonzero_digit = Some(mantissa_digits);
            }
            mantissa_digits += 1;
            i += 1;
        }
        if i == fraction_start {
            return Err(expr_error(invalid_message, path));
        }
    } else if integer_count == 0 {
        return Err(expr_error(invalid_message, path));
    }

    let mut exponent = 0i32;
    let mut explicit_exponent = None;
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        let exponent_message = if invalid_message == "invalid DynamoDB number" {
            "invalid DynamoDB number exponent"
        } else {
            "$numberDecimal requires decimal exponent"
        };
        i += 1;
        let exp_negative = if i < bytes.len() && (bytes[i] == b'-' || bytes[i] == b'+') {
            let negative = bytes[i] == b'-';
            i += 1;
            negative
        } else {
            false
        };
        let exp_start = i;
        let mut exp_value = 0i32;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            exp_value = exp_value
                .checked_mul(10)
                .and_then(|value| value.checked_add((bytes[i] - b'0') as i32))
                .ok_or_else(|| expr_error(exponent_message, path))?;
            i += 1;
        }
        if i == exp_start {
            return Err(expr_error(exponent_message, path));
        }
        exponent = if exp_negative { -exp_value } else { exp_value };
        explicit_exponent = Some(exponent);
    }
    if i != bytes.len() {
        return Err(expr_error(invalid_message, path));
    }

    let significant_digits = match (first_nonzero_digit, last_nonzero_digit) {
        (Some(first), Some(last)) if count_trailing_zeroes => (mantissa_digits - first) as usize,
        (Some(first), Some(last)) => (last - first + 1) as usize,
        _ => 1,
    };
    let adjusted_exponent = first_nonzero_digit.map(|first| {
        exponent
            .saturating_add(integer_digits)
            .saturating_sub(first)
            .saturating_sub(1)
    });
    Ok(DecimalShape {
        significant_digits,
        adjusted_exponent,
        explicit_exponent,
    })
}

pub(in crate::v2_eval::operators::typed_value) fn canonical_decimal(
    raw: &str,
    path: &str,
) -> Result<String, TransformError> {
    let raw = raw.trim_start_matches('+');
    let (negative, unsigned) = raw
        .strip_prefix('-')
        .map_or((false, raw), |rest| (true, rest));
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(index) => {
            let exponent = unsigned[index + 1..]
                .parse::<i64>()
                .map_err(|_| expr_error("invalid DynamoDB number exponent", path))?;
            (&unsigned[..index], exponent)
        }
        None => (unsigned, 0),
    };
    let fractional_digits = mantissa
        .find('.')
        .map_or(0_i64, |index| (mantissa.len() - index - 1) as i64);
    let mut digits = mantissa.chars().filter(|ch| *ch != '.').collect::<String>();
    let trimmed_digits = digits.trim_start_matches('0');
    if trimmed_digits.is_empty() {
        return Ok("0".to_string());
    }
    if trimmed_digits.len() != digits.len() {
        digits = trimmed_digits.to_string();
    }

    let mut scale = fractional_digits - exponent;
    while scale > 0 && digits.ends_with('0') {
        digits.pop();
        scale -= 1;
    }
    if scale < 0 {
        digits.push_str(&"0".repeat((-scale) as usize));
        scale = 0;
    }

    let sign = if negative { "-" } else { "" };
    Ok(format!("{}{}e-{}", sign, digits, scale))
}

pub(in crate::v2_eval::operators::typed_value) fn validate_int64_string(
    raw: &str,
    label: &str,
    path: &str,
) -> Result<(), TransformError> {
    if raw.trim() != raw || raw.is_empty() || raw.parse::<i64>().is_err() {
        return Err(expr_error(format!("{} requires int64 string", label), path));
    }
    Ok(())
}

pub(in crate::v2_eval::operators::typed_value) fn validate_int32_string(
    raw: &str,
    label: &str,
    path: &str,
) -> Result<(), TransformError> {
    if raw.trim() != raw || raw.is_empty() || raw.parse::<i32>().is_err() {
        return Err(expr_error(format!("{} requires int32 string", label), path));
    }
    Ok(())
}

pub(in crate::v2_eval::operators::typed_value) fn validate_finite_f64_string(
    raw: &str,
    label: &str,
    path: &str,
) -> Result<(), TransformError> {
    if raw.trim() != raw || raw.is_empty() {
        return Err(expr_error(
            format!("{} requires finite number string", label),
            path,
        ));
    }
    let parsed = raw
        .parse::<f64>()
        .map_err(|_| expr_error(format!("{} requires finite number string", label), path))?;
    if !parsed.is_finite() {
        return Err(expr_error(
            format!("{} requires finite number string", label),
            path,
        ));
    }
    Ok(())
}

pub(in crate::v2_eval::operators::typed_value) fn validate_firestore_double_string(
    raw: &str,
    path: &str,
) -> Result<(), TransformError> {
    if is_special_double_string(raw) {
        return Ok(());
    }
    validate_finite_f64_string(raw, "doubleValue", path)
}

pub(in crate::v2_eval::operators::typed_value) fn validate_mongo_numeric_wrapper(
    key: &str,
    raw: &str,
    path: &str,
) -> Result<(), TransformError> {
    match key {
        "$numberInt" => validate_int32_string(raw, "$numberInt", path),
        "$numberLong" => validate_int64_string(raw, "$numberLong", path),
        "$numberDouble" => validate_mongo_double_string(raw, path),
        _ => Err(expr_error(
            format!("unsupported MongoDB numeric wrapper: {}", key),
            path,
        )),
    }
}

pub(in crate::v2_eval::operators::typed_value) fn validate_mongo_double_string(
    raw: &str,
    path: &str,
) -> Result<(), TransformError> {
    if is_special_double_string(raw) {
        return Ok(());
    }
    validate_finite_f64_string(raw, "$numberDouble", path)
}

pub(in crate::v2_eval::operators::typed_value) fn is_special_double_string(raw: &str) -> bool {
    matches!(raw, "NaN" | "Infinity" | "-Infinity")
}

pub(in crate::v2_eval::operators::typed_value) fn validate_mongo_decimal128(
    raw: &str,
    path: &str,
) -> Result<(), TransformError> {
    if is_special_double_string(raw) {
        return Ok(());
    }
    let decimal = parse_strict_decimal(
        raw,
        true,
        true,
        "$numberDecimal requires decimal string",
        path,
    )?;
    if decimal.significant_digits > 34 {
        return Err(expr_error(
            "$numberDecimal exceeds 34 significant digits",
            path,
        ));
    }
    if let Some(exponent) = decimal.adjusted_exponent.or(decimal.explicit_exponent)
        && !(-6143..=6144).contains(&exponent)
    {
        return Err(expr_error("$numberDecimal exponent is out of range", path));
    }
    Ok(())
}

pub(in crate::v2_eval::operators::typed_value) fn parse_json_number_if_safe(
    raw: &str,
    path: &str,
) -> Result<JsonNumber, TransformError> {
    validate_dynamodb_number(raw, path)?;
    if raw.contains(['e', 'E']) {
        return Err(expr_error(
            "unsafe provider number exponent cannot be parsed",
            path,
        ));
    }
    let integer_like = !raw.contains('.');
    if integer_like {
        if let Ok(value) = raw.parse::<i64>() {
            let abs = value.unsigned_abs();
            if abs <= 9_007_199_254_740_991 {
                return Ok(JsonNumber::from(value));
            }
        }
        return Err(expr_error(
            "provider number cannot be represented safely as JSON number",
            path,
        ));
    }
    let parsed = raw
        .parse::<f64>()
        .map_err(|_| expr_error("provider number cannot be parsed safely", path))?;
    if !parsed.is_finite() || parsed.to_string() != raw {
        return Err(expr_error(
            "provider number cannot be represented exactly",
            path,
        ));
    }
    JsonNumber::from_f64(parsed).ok_or_else(|| expr_error("provider number is not finite", path))
}
