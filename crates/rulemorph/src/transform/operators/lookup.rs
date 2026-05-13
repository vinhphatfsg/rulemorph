use super::*;

pub(super) fn eval_lookup(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    first_only: bool,
    locals: Option<&EvalLocals<'_>>,
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
    let collection =
        match eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)? {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => value,
        };
    let collection_array = match collection {
        JsonValue::Array(items) => items,
        JsonValue::Null => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "lookup collection must be an array",
            )
            .with_path(collection_path));
        }
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "lookup collection must be an array",
            )
            .with_path(collection_path));
        }
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
    let key_tokens = parse_path(key_path).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "lookup key_path is invalid")
            .with_path(format!("{}.args[1]", base_path))
    })?;

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
        let tokens = parse_path(value).map_err(|_| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path is invalid",
            )
            .with_path(format!("{}.args[3]", base_path))
        })?;
        Some(tokens)
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

    let mut results = Vec::new();
    for item in &collection_array {
        let key_value = match get_path(item, &key_tokens) {
            Some(value) => value,
            None => continue,
        };
        let item_key = match value_to_string_optional(key_value) {
            Some(value) => value,
            None => continue,
        };
        if item_key != match_key {
            continue;
        }

        let selected = match output_tokens.as_ref() {
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
