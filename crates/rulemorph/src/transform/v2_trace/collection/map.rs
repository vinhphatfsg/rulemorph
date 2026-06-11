use super::*;

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn eval_v2_map_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    step_ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let operator = op_step.op.as_str();
    if op_step.args.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "map requires exactly one argument",
        )
        .with_path(path));
    }
    let array = v2_eval_array_from_value(pipe_value, path)?;
    let arg_path = format!("{}.args[0]", path);
    let limits = step_ctx.limits();
    let mut generated_items = 0usize;
    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_path = format!("{}[{}]", path, index);
        emit_v2_collection_item_start(collector, &item_path, operator, index, item);
        let item_ctx = step_ctx
            .clone()
            .with_pipe_value(V2EvalValue::Value(item.clone()))
            .with_item(V2EvalItem { value: item, index });
        let value = eval_v2_expr_traced(
            &op_step.args[0],
            record,
            context,
            out,
            &arg_path,
            &item_ctx,
            collector,
        )?;
        emit_v2_arg_eval(collector, &arg_path, 0, operator, &value);
        finish_v2_collection_item(collector, &item_path, operator, index, &value, None);
        if let V2EvalValue::Value(value) = value {
            push_generated_array_item(&mut results, value, limits, path, &mut generated_items)?;
        }
    }
    Ok(V2EvalValue::Value(JsonValue::Array(results)))
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn eval_v2_flat_map_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    step_ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let operator = op_step.op.as_str();
    if op_step.args.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "flat_map requires exactly one argument",
        )
        .with_path(path));
    }
    let array = v2_eval_array_from_value(pipe_value, path)?;
    let arg_path = format!("{}.args[0]", path);
    let limits = step_ctx.limits();
    let mut generated_items = 0usize;
    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_path = format!("{}[{}]", path, index);
        emit_v2_collection_item_start(collector, &item_path, operator, index, item);
        let item_ctx = step_ctx
            .clone()
            .with_pipe_value(V2EvalValue::Value(item.clone()))
            .with_item(V2EvalItem { value: item, index });
        let value = eval_v2_expr_or_null_traced(
            &op_step.args[0],
            record,
            context,
            out,
            &arg_path,
            &item_ctx,
            collector,
        )?;
        let output = V2EvalValue::Value(value.clone());
        emit_v2_arg_eval(collector, &arg_path, 0, operator, &output);
        finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
        match value {
            JsonValue::Array(items) => extend_generated_array_items(
                &mut results,
                items,
                limits,
                path,
                &mut generated_items,
            )?,
            value => {
                push_generated_array_item(&mut results, value, limits, path, &mut generated_items)?
            }
        }
    }
    Ok(V2EvalValue::Value(JsonValue::Array(results)))
}
