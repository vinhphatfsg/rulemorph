use super::*;

pub(in crate::transform) fn eval_concat(
    expr_op: &ExprOp,
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(&expr_op.args, injected);
    let mut parts = Vec::new();
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index(
            index,
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        )?;
        match value {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => {
                if value.is_null() {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "concat does not accept null",
                    )
                    .with_path(arg_path));
                }
                let part = value_to_string(&value, &arg_path)?;
                parts.push(part);
            }
        }
    }
    Ok(EvalValue::Value(JsonValue::String(parts.join(""))))
}

pub(in crate::transform) fn eval_coalesce(
    expr_op: &ExprOp,
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(&expr_op.args, injected);
    for index in 0..total_len {
        let value = eval_expr_at_index(
            index,
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        )?;
        match value {
            EvalValue::Missing => continue,
            EvalValue::Value(value) => {
                if value.is_null() {
                    continue;
                }
                return Ok(EvalValue::Value(value));
            }
        }
    }
    Ok(EvalValue::Missing)
}
