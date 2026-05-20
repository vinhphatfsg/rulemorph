use super::*;

pub(super) fn eval_reduce<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    if op_step.args.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "reduce requires exactly one argument",
        )
        .with_path(path));
    }
    let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }
    let expr_path = format!("{}.args[0]", path);
    let mut acc = array[0].clone();
    for (index, item) in array.iter().enumerate().skip(1) {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index })
            .with_acc(&acc);
        let value = eval_v2_expr_or_null(
            &op_step.args[0],
            record,
            context,
            out,
            &expr_path,
            &item_ctx,
        )?;
        acc = value;
    }
    Ok(EvalValue::Value(acc))
}

pub(super) fn eval_fold<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    if op_step.args.len() != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "fold requires exactly two arguments",
        )
        .with_path(path));
    }
    let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
    let init_path = format!("{}.args[0]", path);
    let initial = match eval_v2_expr(&op_step.args[0], record, context, out, &init_path, ctx)? {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    let expr_path = format!("{}.args[1]", path);
    let mut acc = initial;
    for (index, item) in array.iter().enumerate() {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index })
            .with_acc(&acc);
        let value = eval_v2_expr_or_null(
            &op_step.args[1],
            record,
            context,
            out,
            &expr_path,
            &item_ctx,
        )?;
        acc = value;
    }
    Ok(EvalValue::Value(acc))
}
