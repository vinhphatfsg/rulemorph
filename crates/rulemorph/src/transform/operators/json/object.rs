use super::*;

pub(in crate::transform::operators) fn eval_json_keys(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |map| {
            Ok(JsonValue::Array(
                map.keys().cloned().map(JsonValue::String).collect(),
            ))
        },
    )
}

pub(in crate::transform::operators) fn eval_json_values(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |map| Ok(JsonValue::Array(map.values().cloned().collect())),
    )
}

pub(in crate::transform::operators) fn eval_json_entries(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |map| {
            let mut entries = Vec::with_capacity(map.len());
            for (key, value) in map {
                let mut entry = Map::new();
                entry.insert("key".to_string(), JsonValue::String(key.clone()));
                entry.insert("value".to_string(), value.clone());
                entries.push(JsonValue::Object(entry));
            }
            Ok(JsonValue::Array(entries))
        },
    )
}

pub(in crate::transform::operators) fn eval_json_from_entries(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=2).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one or two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let first_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let first_value = match first_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if first_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(&arg_path));
    }

    if total_len == 1 {
        return match first_value {
            JsonValue::Object(map) => Ok(EvalValue::Value(JsonValue::Object(map))),
            JsonValue::Array(items) => {
                let mut output = Map::new();
                for (index, item) in items.iter().enumerate() {
                    let entry_path = format!("{}[{}]", arg_path, index);
                    match item {
                        JsonValue::Array(pair) => {
                            if pair.len() != 2 {
                                return Err(TransformError::new(
                                    TransformErrorKind::ExprError,
                                    "entries must have exactly two items",
                                )
                                .with_path(&entry_path));
                            }
                            let key_path = format!("{}[0]", entry_path);
                            let key = value_to_string(&pair[0], &key_path)?;
                            let value = pair[1].clone();
                            output.insert(key, value);
                        }
                        JsonValue::Object(map) => {
                            let key_path = format!("{}.key", entry_path);
                            let value_path = format!("{}.value", entry_path);
                            let key_value = map.get("key").ok_or_else(|| {
                                TransformError::new(
                                    TransformErrorKind::ExprError,
                                    "entry must contain key",
                                )
                                .with_path(&key_path)
                            })?;
                            if key_value.is_null() {
                                return Err(TransformError::new(
                                    TransformErrorKind::ExprError,
                                    "entry key must not be null",
                                )
                                .with_path(&key_path));
                            }
                            let value_value = map.get("value").ok_or_else(|| {
                                TransformError::new(
                                    TransformErrorKind::ExprError,
                                    "entry must contain value",
                                )
                                .with_path(&value_path)
                            })?;
                            let key = value_to_string(key_value, &key_path)?;
                            output.insert(key, value_value.clone());
                        }
                        _ => {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "entries must be arrays or objects",
                            )
                            .with_path(&entry_path));
                        }
                    }
                }
                Ok(EvalValue::Value(JsonValue::Object(output)))
            }
            _ => Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object or array",
            )
            .with_path(arg_path)),
        };
    }

    let key = value_to_string(&first_value, &arg_path)?;
    let value =
        match eval_expr_at_index(1, args, injected, record, context, out, base_path, locals)? {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => value,
        };
    let mut output = Map::new();
    output.insert(key, value);
    Ok(EvalValue::Value(JsonValue::Object(output)))
}

pub(in crate::transform::operators) fn eval_json_object_flatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |map| {
            let mut output = Map::new();
            let mut tokens = Vec::new();
            flatten_object(map, &mut tokens, &mut output, base_path)?;
            Ok(JsonValue::Object(output))
        },
    )
}

pub(in crate::transform::operators) fn eval_json_object_unflatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |map| {
            let mut paths = Vec::with_capacity(map.len());
            let mut values = Vec::with_capacity(map.len());
            for (key, value) in map {
                let tokens = parse_path_tokens(
                    key,
                    TransformErrorKind::ExprError,
                    format!("{}.args[0]", base_path),
                )?;
                if tokens
                    .iter()
                    .any(|token| matches!(token, PathToken::Index(_)))
                {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "array indexes are not allowed in path",
                    )
                    .with_path(format!("{}.args[0]", base_path)));
                }
                if has_path_conflict(&paths, &tokens) {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "path conflicts with another path",
                    )
                    .with_path(format!("{}.args[0]", base_path)));
                }
                paths.push(tokens);
                values.push(value.clone());
            }

            let mut root = JsonValue::Object(Map::new());
            for (tokens, value) in paths.into_iter().zip(values) {
                set_path_object_only(&mut root, &tokens, value, base_path)?;
            }

            Ok(root)
        },
    )
}

fn eval_json_object_unary<F>(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    op: F,
) -> Result<EvalValue, TransformError>
where
    F: FnOnce(&Map<String, JsonValue>) -> Result<JsonValue, TransformError>,
{
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
    let map = match value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object",
            )
            .with_path(arg_path));
        }
    };

    op(&map).map(EvalValue::Value)
}
