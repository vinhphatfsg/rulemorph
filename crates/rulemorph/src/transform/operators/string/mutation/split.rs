use super::*;

pub(in crate::transform::operators) fn eval_split(
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

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let delimiter =
        match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let delimiter_path = format!("{}.args[1]", base_path);

    if delimiter.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "split delimiter must not be empty",
        )
        .with_path(delimiter_path));
    }

    let parts = value
        .split(&delimiter)
        .map(|part| JsonValue::String(part.to_string()))
        .collect::<Vec<_>>();

    Ok(EvalValue::Value(JsonValue::Array(parts)))
}
