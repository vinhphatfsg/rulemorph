use super::*;

pub(in crate::transform::operators) fn eval_json_merge(
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
