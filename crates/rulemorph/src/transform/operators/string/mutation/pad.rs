use super::*;

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
