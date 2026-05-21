use super::*;

mod parsing;
mod timezone;

use chrono::FixedOffset;

use self::parsing::{parse_datetime, parse_format_list};
use self::timezone::{looks_like_timezone, parse_timezone};

pub(super) fn eval_date_format(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=4).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two to four items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let output_format =
        match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let value_path = format!("{}.args[0]", base_path);
    let mut input_formats: Option<Vec<String>> = None;
    let mut timezone: Option<FixedOffset> = None;

    if total_len >= 3 {
        let input_path = format!("{}.args[2]", base_path);
        let input_value =
            match eval_arg_value_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if input_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(input_path));
        }

        if let Some(value) = input_value.as_str() {
            if looks_like_timezone(value) {
                timezone = Some(parse_timezone(value, &input_path)?);
            } else {
                input_formats = Some(parse_format_list(&input_value, &input_path)?);
            }
        } else {
            input_formats = Some(parse_format_list(&input_value, &input_path)?);
        }
    }

    if total_len == 4 {
        let tz_path = format!("{}.args[3]", base_path);
        let tz_value =
            match eval_arg_string_at(3, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        timezone = Some(parse_timezone(&tz_value, &tz_path)?);
    }

    let dt = parse_datetime(&value, input_formats.as_deref(), timezone, &value_path)?;
    let dt = match timezone {
        Some(offset) => dt.with_timezone(&offset),
        None => dt,
    };
    let formatted = dt.format(&output_format).to_string();
    Ok(EvalValue::Value(JsonValue::String(formatted)))
}

pub(super) fn eval_to_unixtime(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one to three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let value_path = format!("{}.args[0]", base_path);

    let mut unit = "s".to_string();
    let mut timezone: Option<FixedOffset> = None;

    if total_len >= 2 {
        let arg_path = format!("{}.args[1]", base_path);
        let arg_value =
            match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if total_len == 3 {
            if arg_value != "s" && arg_value != "ms" {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unit must be s or ms",
                )
                .with_path(arg_path));
            }
            unit = arg_value;
        } else if arg_value == "s" || arg_value == "ms" {
            unit = arg_value;
        } else if looks_like_timezone(&arg_value) {
            timezone = Some(parse_timezone(&arg_value, &arg_path)?);
        } else {
            return Err(
                TransformError::new(TransformErrorKind::ExprError, "unit must be s or ms")
                    .with_path(arg_path),
            );
        }
    }

    if total_len == 3 {
        let tz_path = format!("{}.args[2]", base_path);
        let tz_value =
            match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        timezone = Some(parse_timezone(&tz_value, &tz_path)?);
    }

    let dt = parse_datetime(&value, None, timezone, &value_path)?;
    let dt = match timezone {
        Some(offset) => dt.with_timezone(&offset),
        None => dt,
    };
    let timestamp = if unit == "ms" {
        dt.timestamp_millis()
    } else {
        dt.timestamp()
    };

    Ok(EvalValue::Value(JsonValue::Number(timestamp.into())))
}
