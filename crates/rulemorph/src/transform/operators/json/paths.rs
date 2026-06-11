use super::*;

#[expect(
    clippy::too_many_arguments,
    reason = "operator eval helpers keep the shared v1 expression call shape until a wider evaluator context rewrite"
)]
pub(super) fn eval_json_paths_arg(
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
