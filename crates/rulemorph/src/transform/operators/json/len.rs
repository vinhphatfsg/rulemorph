use super::*;

pub(in crate::transform::operators) fn eval_len(
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
