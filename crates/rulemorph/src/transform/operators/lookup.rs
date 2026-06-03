use super::*;

pub(in crate::transform) fn eval_lookup(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    first_only: bool,
    locals: Option<&EvalLocals<'_>>,
    compiled_lookup: Option<&CompiledLookup>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(3..=4).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup args must be [collection, key_path, match_value, output_path?]",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let collection_path = format!("{}.args[0]", base_path);
    let collection_storage;
    let collection_array =
        if let (None, Some(compiled), Some(context)) = (injected, compiled_lookup, context) {
            if let Some(tokens) = compiled.context_collection_tokens(args) {
                match get_path(context, tokens) {
                    Some(JsonValue::Array(items)) => items.as_slice(),
                    Some(JsonValue::Null) => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "lookup collection must be an array",
                        )
                        .with_path(collection_path));
                    }
                    Some(_) => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "lookup collection must be an array",
                        )
                        .with_path(collection_path));
                    }
                    None => return Ok(EvalValue::Missing),
                }
            } else {
                collection_storage = eval_lookup_collection(
                    0,
                    args,
                    injected,
                    record,
                    Some(context),
                    out,
                    base_path,
                    locals,
                    &collection_path,
                )?;
                let Some(items) = collection_storage.as_ref() else {
                    return Ok(EvalValue::Missing);
                };
                items.as_slice()
            }
        } else {
            collection_storage = eval_lookup_collection(
                0,
                args,
                injected,
                record,
                context,
                out,
                base_path,
                locals,
                &collection_path,
            )?;
            let Some(items) = collection_storage.as_ref() else {
                return Ok(EvalValue::Missing);
            };
            items.as_slice()
        };

    let key_expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let key_path = literal_string(key_expr).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    if key_path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(format!("{}.args[1]", base_path)));
    }
    let key_tokens_storage;
    let key_tokens = if let (None, Some(compiled)) = (injected, compiled_lookup) {
        compiled.key_tokens(args)?
    } else {
        key_tokens_storage = parse_path(key_path).map_err(|_| {
            TransformError::new(TransformErrorKind::ExprError, "lookup key_path is invalid")
                .with_path(format!("{}.args[1]", base_path))
        })?;
        &key_tokens_storage
    };

    let output_tokens_storage;
    let output_tokens = if total_len == 4 {
        let output_expr = arg_expr_at(3, args, injected).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path must be a non-empty string literal",
            )
            .with_path(format!("{}.args[3]", base_path))
        })?;
        let value = literal_string(output_expr).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path must be a non-empty string literal",
            )
            .with_path(format!("{}.args[3]", base_path))
        })?;
        if value.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path must be a non-empty string literal",
            )
            .with_path(format!("{}.args[3]", base_path)));
        }
        if let (None, Some(compiled)) = (injected, compiled_lookup) {
            compiled.output_tokens(args).transpose()?
        } else {
            let tokens = parse_path(value).map_err(|_| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "lookup output_path is invalid",
                )
                .with_path(format!("{}.args[3]", base_path))
            })?;
            output_tokens_storage = tokens;
            Some(output_tokens_storage.as_slice())
        }
    } else {
        None
    };

    let match_path = format!("{}.args[2]", base_path);
    let match_value =
        match eval_expr_at_index(2, args, injected, record, context, out, base_path, locals)? {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => value,
        };
    if match_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup match_value must not be null",
        )
        .with_path(match_path));
    }
    let match_key = value_to_string(&match_value, &match_path)?;

    if let (None, Some(compiled)) = (injected, compiled_lookup) {
        if let Some(index) = compiled.index(&args, collection_array, key_tokens, output_tokens) {
            let Some(matches) = index.get(&match_key) else {
                return Ok(EvalValue::Missing);
            };
            match matches {
                LookupMatches::ClonedValues(values) => {
                    if first_only {
                        return Ok(EvalValue::Value(values[0].clone()));
                    }
                    return Ok(EvalValue::Value(JsonValue::Array(values.to_vec())));
                }
                LookupMatches::ItemIndices(indices) => {
                    let mut results = Vec::new();
                    for &index in indices {
                        let Some(item) = collection_array.get(index) else {
                            continue;
                        };
                        let selected = match output_tokens {
                            Some(tokens) => get_path(item, tokens),
                            None => Some(item),
                        };
                        if let Some(value) = selected {
                            if first_only {
                                return Ok(EvalValue::Value(value.clone()));
                            }
                            results.push(value.clone());
                        }
                    }
                    if results.is_empty() {
                        return Ok(EvalValue::Missing);
                    }
                    return Ok(EvalValue::Value(JsonValue::Array(results)));
                }
            }
        }
    }

    let mut results = Vec::new();
    for item in collection_array {
        let key_value = match get_path(item, &key_tokens) {
            Some(value) => value,
            None => continue,
        };
        if !value_matches_string_key(key_value, &match_key) {
            continue;
        }

        let selected = match output_tokens {
            Some(tokens) => get_path(item, tokens),
            None => Some(item),
        };

        if let Some(value) = selected {
            if first_only {
                return Ok(EvalValue::Value(value.clone()));
            }
            results.push(value.clone());
        }
    }

    if results.is_empty() {
        Ok(EvalValue::Missing)
    } else {
        Ok(EvalValue::Value(JsonValue::Array(results)))
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_lookup_collection(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collection_path: &str,
) -> Result<Option<Vec<JsonValue>>, TransformError> {
    let collection = match eval_expr_at_index(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        EvalValue::Missing => return Ok(None),
        EvalValue::Value(value) => value,
    };
    match collection {
        JsonValue::Array(items) => Ok(Some(items)),
        JsonValue::Null => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup collection must be an array",
        )
        .with_path(collection_path)),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup collection must be an array",
        )
        .with_path(collection_path)),
    }
}
