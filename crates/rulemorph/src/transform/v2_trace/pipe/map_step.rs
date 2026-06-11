use super::*;

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn eval_v2_map_step_traced<'a>(
    map: &crate::v2_model::V2MapStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    step_path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<(V2EvalValue, V2EvalContext<'a>), TransformError> {
    collector
        .start_span(TraceEventKind::OpStart, TracePhase::Start)
        .rule_path(step_path)
        .operator("map")
        .input_v2_eval_value(&pipe_value, collector.options(), None)
        .finish(collector);
    let arr = match &pipe_value {
        V2EvalValue::Missing => {
            collector
                .end_span(TraceEventKind::OpEnd, TracePhase::End)
                .rule_path(step_path)
                .operator("map")
                .finish_with_v2_eval_output(collector, &V2EvalValue::Missing, None);
            return Ok((V2EvalValue::Missing, ctx.clone()));
        }
        V2EvalValue::Value(JsonValue::Array(arr)) => arr,
        V2EvalValue::Value(_) => {
            collector
                .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                .rule_path(step_path)
                .operator("map")
                .finish(collector);
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "map step requires array",
            )
            .with_path(step_path));
        }
    };
    let limits = ctx.limits();
    let mut generated_items = 0usize;
    let mut generated_json = crate::transform::GeneratedArrayBudget::new(limits, step_path)
        .inspect_err(|_error| {
            collector
                .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                .rule_path(step_path)
                .operator("map")
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .finish(collector);
        })?;
    let mut results = Vec::with_capacity(arr.len());
    for (index, item_value) in arr.iter().enumerate() {
        let item_path = format!("{}[{}]", step_path, index);
        let item_eval_value = V2EvalValue::Value(item_value.clone());
        let item_ctx = ctx
            .clone()
            .with_pipe_value(item_eval_value.clone())
            .with_item(V2EvalItem {
                value: item_value,
                index,
            });
        let mut current = item_eval_value;
        let mut step_ctx = item_ctx.clone();

        collector
            .start_span(TraceEventKind::CollectionItemStart, TracePhase::Start)
            .rule_path(&item_path)
            .input_path(canonical_item_path(""))
            .attr_index("item_index", index)
            .attr_enum("scope", "item")
            .input_value(item_value, collector.options(), Some("@item"))
            .finish(collector);

        for (nested_index, nested_step) in map.steps.iter().enumerate() {
            let nested_ctx = step_ctx.clone().with_pipe_value(current.clone());
            let (next, next_ctx) = match eval_v2_step_traced(
                nested_step,
                current,
                record,
                context,
                out,
                &format!("{}.step[{}]", item_path, nested_index),
                &nested_ctx,
                collector,
            ) {
                Ok(result) => result,
                Err(error) => {
                    collector
                        .error_span(TraceEventKind::Error, "COLLECTION_ERROR", "item failed")
                        .rule_path(&item_path)
                        .finish(collector);
                    collector
                        .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                        .rule_path(step_path)
                        .operator("map")
                        .input_v2_eval_value(&pipe_value, collector.options(), None)
                        .finish(collector);
                    return Err(error);
                }
            };
            current = next;
            step_ctx = next_ctx;
        }
        collector
            .end_span(TraceEventKind::CollectionItemEnd, TracePhase::End)
            .rule_path(&item_path)
            .finish_with_v2_eval_output(collector, &current, Some("@item"));
        if let V2EvalValue::Value(value) = current {
            if let Err(error) = generated_json.try_push_value(&value, limits, step_path) {
                collector
                    .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                    .rule_path(step_path)
                    .operator("map")
                    .input_v2_eval_value(&pipe_value, collector.options(), None)
                    .finish(collector);
                return Err(error);
            }
            if let Err(error) = push_generated_array_item(
                &mut results,
                value,
                limits,
                step_path,
                &mut generated_items,
            ) {
                collector
                    .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                    .rule_path(step_path)
                    .operator("map")
                    .input_v2_eval_value(&pipe_value, collector.options(), None)
                    .finish(collector);
                return Err(error);
            }
        }
    }
    collector
        .end_span(TraceEventKind::OpEnd, TracePhase::End)
        .rule_path(step_path)
        .operator("map")
        .finish_with_v2_eval_output(
            collector,
            &V2EvalValue::Value(JsonValue::Array(results.clone())),
            None,
        );
    Ok((V2EvalValue::Value(JsonValue::Array(results)), ctx.clone()))
}
