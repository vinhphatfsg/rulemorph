use super::*;

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

pub(super) fn eval_json_keys(
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

pub(super) fn eval_json_values(
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

pub(super) fn eval_json_entries(
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

pub(super) fn eval_json_from_entries(
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

pub(super) fn eval_json_object_flatten(
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

pub(super) fn eval_json_object_unflatten(
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

fn eval_json_paths_arg(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    index: usize,
    allow_terminal_index: bool,
) -> Result<Option<Vec<Vec<PathToken>>>, TransformError> {
    let arg_path = format!("{}.args[{}]", base_path, index);
    let value = eval_expr_at_index(
        index, args, injected, record, context, out, base_path, locals,
    )?;
    let value = match value {
        EvalValue::Missing => return Ok(None),
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(arg_path));
    }
    let items: Vec<(String, String)> = match value {
        JsonValue::String(path) => vec![(arg_path.clone(), path)],
        JsonValue::Array(items) => items
            .iter()
            .enumerate()
            .map(|(path_index, item)| {
                let item_path = format!("{}.args[{}][{}]", base_path, index, path_index);
                let path = item.as_str().ok_or_else(|| {
                    TransformError::new(
                        TransformErrorKind::ExprError,
                        "paths must be a string or array of strings",
                    )
                    .with_path(&item_path)
                })?;
                Ok::<(String, String), TransformError>((item_path, path.to_string()))
            })
            .collect::<Result<Vec<_>, TransformError>>()?,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "paths must be a string or array of strings",
            )
            .with_path(arg_path));
        }
    };

    let mut paths = Vec::new();
    for (item_path, path) in items {
        let tokens = parse_path_tokens(&path, TransformErrorKind::ExprError, &item_path)?;
        if !allow_terminal_index && matches!(tokens.last(), Some(PathToken::Index(_))) {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "path must not end with array index",
            )
            .with_path(item_path));
        }
        if has_duplicate_path(&paths, &tokens) {
            continue;
        }
        if has_path_conflict(&paths, &tokens) {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "path conflicts with another path",
            )
            .with_path(item_path));
        }
        paths.push(tokens);
    }

    Ok(Some(paths))
}
