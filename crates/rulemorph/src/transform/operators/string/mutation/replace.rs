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
