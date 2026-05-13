use super::*;

use chrono::offset::TimeZone;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use std::sync::OnceLock;

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

const DEFAULT_DATE_FORMATS_WITH_TZ: [&str; 8] = [
    "%Y-%m-%dT%H:%M:%S%:z",
    "%Y-%m-%d %H:%M:%S%:z",
    "%Y-%m-%dT%H:%M:%S%.f%:z",
    "%Y-%m-%d %H:%M:%S%.f%:z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y/%m/%d %H:%M:%S%:z",
    "%Y/%m/%d %H:%M:%S%z",
];

const DEFAULT_DATE_FORMATS: [&str; 12] = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y%m%d",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y/%m/%d %H:%M:%S%.f",
];

fn parse_format_list(value: &JsonValue, path: &str) -> Result<Vec<String>, TransformError> {
    match value {
        JsonValue::String(s) => {
            if s.is_empty() {
                Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "input_format must not be empty",
                )
                .with_path(path))
            } else {
                Ok(vec![s.clone()])
            }
        }
        JsonValue::Array(items) => {
            if items.is_empty() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "input_format must not be empty",
                )
                .with_path(path));
            }
            let mut formats = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                let value = match item.as_str() {
                    Some(value) => value,
                    None => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "input_format must be a string or array of strings",
                        )
                        .with_path(item_path));
                    }
                };
                if value.is_empty() {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "input_format must not be empty",
                    )
                    .with_path(item_path));
                }
                formats.push(value.to_string());
            }
            Ok(formats)
        }
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "input_format must be a string or array of strings",
        )
        .with_path(path)),
    }
}

fn parse_datetime(
    value: &str,
    formats: Option<&[String]>,
    timezone: Option<FixedOffset>,
    path: &str,
) -> Result<DateTime<FixedOffset>, TransformError> {
    if let Some(formats) = formats {
        return parse_datetime_with_formats(value, formats, timezone, path);
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt);
    }
    if let Ok(dt) = DateTime::parse_from_rfc2822(value) {
        return Ok(dt);
    }

    for format in DEFAULT_DATE_FORMATS_WITH_TZ {
        if let Ok(dt) = DateTime::parse_from_str(value, format) {
            return Ok(dt);
        }
    }

    parse_datetime_with_formats(value, default_date_formats(), timezone, path)
}

fn default_date_formats() -> &'static [String] {
    static DEFAULT_DATE_FORMATS_CACHE: OnceLock<Vec<String>> = OnceLock::new();
    DEFAULT_DATE_FORMATS_CACHE
        .get_or_init(|| {
            DEFAULT_DATE_FORMATS
                .iter()
                .map(|format| format.to_string())
                .collect()
        })
        .as_slice()
}

fn parse_datetime_with_formats(
    value: &str,
    formats: &[String],
    timezone: Option<FixedOffset>,
    path: &str,
) -> Result<DateTime<FixedOffset>, TransformError> {
    for format in formats {
        if let Ok(dt) = DateTime::parse_from_str(value, format) {
            return Ok(dt);
        }
        if let Ok(naive) = NaiveDateTime::parse_from_str(value, format) {
            return apply_timezone(naive, timezone, path);
        }
        if let Ok(date) = NaiveDate::parse_from_str(value, format) {
            let naive = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| expr_type_error("date is invalid", path))?;
            return apply_timezone(naive, timezone, path);
        }
    }

    Err(
        TransformError::new(TransformErrorKind::ExprError, "date format is invalid")
            .with_path(path),
    )
}

fn apply_timezone(
    naive: NaiveDateTime,
    timezone: Option<FixedOffset>,
    path: &str,
) -> Result<DateTime<FixedOffset>, TransformError> {
    let offset = timezone.unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
    offset
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| expr_type_error("date is invalid", path))
}

fn looks_like_timezone(value: &str) -> bool {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return true;
    }
    matches!(value.chars().next(), Some('+') | Some('-'))
}

fn parse_timezone(value: &str, path: &str) -> Result<FixedOffset, TransformError> {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return FixedOffset::east_opt(0).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "timezone must be UTC or an offset like +09:00",
            )
            .with_path(path)
        });
    }

    let (sign, rest) = match value.chars().next() {
        Some('+') => (1i32, &value[1..]),
        Some('-') => (-1i32, &value[1..]),
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "timezone must be UTC or an offset like +09:00",
            )
            .with_path(path));
        }
    };

    let (hours, minutes) = if let Some((h, m)) = rest.split_once(':') {
        let hours = h.parse::<i32>().ok();
        let minutes = m.parse::<i32>().ok();
        match (hours, minutes) {
            (Some(hours), Some(minutes)) => (hours, minutes),
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "timezone must be UTC or an offset like +09:00",
                )
                .with_path(path));
            }
        }
    } else {
        match rest.len() {
            2 => {
                let hours = rest.parse::<i32>().ok();
                match hours {
                    Some(hours) => (hours, 0),
                    None => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "timezone must be UTC or an offset like +09:00",
                        )
                        .with_path(path));
                    }
                }
            }
            4 => {
                let hours = rest[..2].parse::<i32>().ok();
                let minutes = rest[2..].parse::<i32>().ok();
                match (hours, minutes) {
                    (Some(hours), Some(minutes)) => (hours, minutes),
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "timezone must be UTC or an offset like +09:00",
                        )
                        .with_path(path));
                    }
                }
            }
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "timezone must be UTC or an offset like +09:00",
                )
                .with_path(path));
            }
        }
    };

    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "timezone must be UTC or an offset like +09:00",
        )
        .with_path(path));
    }

    let offset_seconds = sign * (hours * 3600 + minutes * 60);
    FixedOffset::east_opt(offset_seconds).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "timezone must be UTC or an offset like +09:00",
        )
        .with_path(path)
    })
}
