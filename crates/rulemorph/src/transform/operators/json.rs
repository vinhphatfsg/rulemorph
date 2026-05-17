use super::*;

mod object;
mod paths;

pub(super) use self::object::{
    eval_json_entries, eval_json_from_entries, eval_json_keys, eval_json_object_flatten,
    eval_json_object_unflatten, eval_json_values,
};
use self::paths::eval_json_paths_arg;

pub(super) fn eval_json_merge(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    deep: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut result: Option<Map<String, JsonValue>> = None;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index(
            index, args, injected, record, context, out, base_path, locals,
        )?;
        let value = match value {
            EvalValue::Missing => continue,
            EvalValue::Value(value) => value,
        };
        if value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(arg_path));
        }
        let obj = match value {
            JsonValue::Object(map) => map,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr arg must be object",
                )
                .with_path(arg_path));
            }
        };

        match result {
            Some(ref mut existing) => merge_object(existing, &obj, deep),
            None => result = Some(obj),
        }
    }

    match result {
        Some(map) => Ok(EvalValue::Value(JsonValue::Object(map))),
        None => Ok(EvalValue::Missing),
    }
}

pub(super) fn eval_json_get(
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

    let base_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let base_value = match base_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if base_value.is_null() {
        return Ok(EvalValue::Missing);
    }

    let path_path = format!("{}.args[1]", base_path);
    let path_value =
        eval_expr_at_index(1, args, injected, record, context, out, base_path, locals)?;
    let path_value = match path_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if path_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path_path));
    }
    let path = value_as_string(&path_value, &path_path)?;
    if path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "path must be a non-empty string",
        )
        .with_path(path_path));
    }
    let tokens = parse_path_tokens(&path, TransformErrorKind::ExprError, &path_path)?;
    match get_path(&base_value, &tokens) {
        Some(value) => Ok(EvalValue::Value(value.clone())),
        None => Ok(EvalValue::Missing),
    }
}

pub(super) fn eval_json_pick(
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

pub(super) fn eval_json_omit(
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

pub(super) fn eval_len(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value = eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let value = match value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(arg_path));
    }

    let len = match value {
        JsonValue::String(value) => value.chars().count(),
        JsonValue::Array(items) => items.len(),
        JsonValue::Object(map) => map.len(),
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be string, array, or object",
            )
            .with_path(arg_path));
        }
    };

    Ok(EvalValue::Value(JsonValue::Number(
        serde_json::Number::from(len as u64),
    )))
}
