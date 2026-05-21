use super::*;

#[derive(Clone, Copy)]
enum ReplaceMode {
    LiteralFirst,
    LiteralAll,
    RegexFirst,
    RegexAll,
}

fn parse_replace_mode(value: &str, path: &str) -> Result<ReplaceMode, TransformError> {
    match value {
        "all" => Ok(ReplaceMode::LiteralAll),
        "regex" => Ok(ReplaceMode::RegexFirst),
        "regex_all" => Ok(ReplaceMode::RegexAll),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "replace mode must be all|regex|regex_all",
        )
        .with_path(path)),
    }
}

pub(in crate::transform::operators) fn eval_replace(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(3..=4).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain three or four items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let pattern =
        match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let replacement =
        match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let pattern_path = format!("{}.args[1]", base_path);

    let mode = if total_len == 4 {
        let mode_path = format!("{}.args[3]", base_path);
        let mode_value =
            match eval_arg_string_at(3, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        parse_replace_mode(&mode_value, &mode_path)?
    } else {
        ReplaceMode::LiteralFirst
    };

    let replaced = match mode {
        ReplaceMode::LiteralFirst => value.replacen(&pattern, &replacement, 1),
        ReplaceMode::LiteralAll => value.replace(&pattern, &replacement),
        ReplaceMode::RegexFirst => {
            let regex = cached_regex(&pattern, &pattern_path)?;
            regex.replace(&value, replacement.as_str()).to_string()
        }
        ReplaceMode::RegexAll => {
            let regex = cached_regex(&pattern, &pattern_path)?;
            regex.replace_all(&value, replacement.as_str()).to_string()
        }
    };

    Ok(EvalValue::Value(JsonValue::String(replaced)))
}

pub(in crate::transform::operators) fn eval_split(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let delimiter =
        match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let delimiter_path = format!("{}.args[1]", base_path);

    if delimiter.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "split delimiter must not be empty",
        )
        .with_path(delimiter_path));
    }

    let parts = value
        .split(&delimiter)
        .map(|part| JsonValue::String(part.to_string()))
        .collect::<Vec<_>>();

    Ok(EvalValue::Value(JsonValue::Array(parts)))
}

pub(in crate::transform::operators) fn eval_pad(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    pad_start: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two or three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };

    let length_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let length_path = format!("{}.args[1]", base_path);
    if length_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(length_path));
    }
    let length = value_to_i64(
        &length_value,
        &length_path,
        "pad length must be a non-negative integer",
    )?;
    if length < 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "pad length must be a non-negative integer",
        )
        .with_path(length_path));
    }

    let pad_string = if total_len == 3 {
        match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        }
    } else {
        " ".to_string()
    };

    let target_len = usize::try_from(length).map_err(|_| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "pad length must be a non-negative integer",
        )
        .with_path(length_path)
    })?;

    let padded = pad_string_value(&value, target_len, &pad_string, pad_start);
    Ok(EvalValue::Value(JsonValue::String(padded)))
}

fn pad_string_value(value: &str, target_len: usize, pad: &str, pad_start: bool) -> String {
    let value_len = value.chars().count();
    if value_len >= target_len || pad.is_empty() {
        return value.to_string();
    }

    let needed = target_len - value_len;
    let pad_len = pad.chars().count();
    let repeats = (needed + pad_len - 1) / pad_len;
    let pad_buf = pad.repeat(repeats);
    let pad_slice = pad_buf.chars().take(needed).collect::<String>();

    if pad_start {
        format!("{}{}", pad_slice, value)
    } else {
        format!("{}{}", value, pad_slice)
    }
}
