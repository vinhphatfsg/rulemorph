use super::paths::eval_json_paths_arg;
use super::*;

pub(in crate::transform::operators) fn eval_json_pick(
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

    let base_path_arg = format!("{}.args[0]", base_path);
    let base_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let base_value = match base_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if base_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path_arg));
    }
    let base_obj = match base_value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object",
            )
            .with_path(base_path_arg));
        }
    };
    let base_value = JsonValue::Object(base_obj);

    let paths = eval_json_paths_arg(
        args, injected, record, context, out, base_path, locals, 1, true,
    )?;
    let Some(paths) = paths else {
        return Ok(EvalValue::Missing);
    };

    let mut output = JsonValue::Object(Map::new());
    for tokens in paths {
        if let Some(value) = get_path(&base_value, &tokens) {
            set_path_with_indexes(&mut output, &tokens, value.clone(), base_path)?;
        }
    }

    Ok(EvalValue::Value(output))
}

pub(in crate::transform::operators) fn eval_json_omit(
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

    let base_path_arg = format!("{}.args[0]", base_path);
    let base_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let mut base_value = match base_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if base_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path_arg));
    }
    let base_obj = match base_value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object",
            )
            .with_path(base_path_arg));
        }
    };
    base_value = JsonValue::Object(base_obj);

    let paths = eval_json_paths_arg(
        args, injected, record, context, out, base_path, locals, 1, false,
    )?;
    let Some(paths) = paths else {
        return Ok(EvalValue::Missing);
    };

    for tokens in paths {
        remove_path(&mut base_value, &tokens);
    }

    Ok(EvalValue::Value(base_value))
}
