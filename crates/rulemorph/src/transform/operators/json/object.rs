use super::*;

mod entries;

pub(in crate::transform::operators) use entries::{eval_json_entries, eval_json_from_entries};

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

#[expect(
    clippy::too_many_arguments,
    reason = "operator eval helpers keep the shared v1 expression call shape until a wider evaluator context rewrite"
)]
pub(super) fn eval_json_object_unary<F>(
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
