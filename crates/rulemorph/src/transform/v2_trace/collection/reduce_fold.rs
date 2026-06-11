use super::*;

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn eval_v2_reduce_fold_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    match op_step.op.as_str() {
        "reduce" => eval_v2_reduce_traced(
            op_step, pipe_value, record, context, out, path, ctx, collector,
        ),
        "fold" => eval_v2_fold_traced(
            op_step, pipe_value, record, context, out, path, ctx, collector,
        ),
        _ => unreachable!("non reduce/fold operator dispatched to reduce_fold"),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
fn eval_v2_reduce_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let operator = "reduce";
    if op_step.args.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "reduce requires exactly one argument",
        )
        .with_path(path));
    }
    let array = v2_eval_array_from_value(pipe_value, path)?;
    if array.is_empty() {
        return Ok(V2EvalValue::Value(JsonValue::Null));
    }
    let expr_path = format!("{}.args[0]", path);
    let mut acc = array[0].clone();
    for (index, item) in array.iter().enumerate().skip(1) {
        let item_path = format!("{}[{}]", path, index);
        emit_v2_collection_item_start(collector, &item_path, operator, index, item);
        let item_ctx = ctx
            .clone()
            .with_pipe_value(V2EvalValue::Value(item.clone()))
            .with_item(V2EvalItem { value: item, index })
            .with_acc(&acc);
        let value = eval_v2_expr_or_null_traced(
            &op_step.args[0],
            record,
            context,
            out,
            &expr_path,
            &item_ctx,
            collector,
        )?;
        let output = V2EvalValue::Value(value.clone());
        emit_v2_arg_eval(collector, &expr_path, 0, operator, &output);
        acc = value;
        finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
    }
    Ok(V2EvalValue::Value(acc))
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
fn eval_v2_fold_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let operator = "fold";
    if op_step.args.len() != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "fold requires exactly two arguments",
        )
        .with_path(path));
    }
    let array = v2_eval_array_from_value(pipe_value, path)?;
    let init_path = format!("{}.args[0]", path);
    let initial = eval_v2_expr_traced(
        &op_step.args[0],
        record,
        context,
        out,
        &init_path,
        ctx,
        collector,
    )?;
    emit_v2_arg_eval(collector, &init_path, 0, operator, &initial);
    let mut acc = match initial {
        V2EvalValue::Missing => return Ok(V2EvalValue::Missing),
        V2EvalValue::Value(value) => value,
    };
    let expr_path = format!("{}.args[1]", path);
    for (index, item) in array.iter().enumerate() {
        let item_path = format!("{}[{}]", path, index);
        emit_v2_collection_item_start(collector, &item_path, operator, index, item);
        let item_ctx = ctx
            .clone()
            .with_pipe_value(V2EvalValue::Value(item.clone()))
            .with_item(V2EvalItem { value: item, index })
            .with_acc(&acc);
        let value = eval_v2_expr_or_null_traced(
            &op_step.args[1],
            record,
            context,
            out,
            &expr_path,
            &item_ctx,
            collector,
        )?;
        let output = V2EvalValue::Value(value.clone());
        emit_v2_arg_eval(collector, &expr_path, 1, operator, &output);
        acc = value;
        finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
    }
    Ok(V2EvalValue::Value(acc))
}
