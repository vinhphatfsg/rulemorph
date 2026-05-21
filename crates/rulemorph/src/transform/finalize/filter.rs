use super::*;

pub(super) fn apply_filter(
    records: &mut Vec<JsonValue>,
    filter: &Expr,
    context: Option<&JsonValue>,
) -> Result<(), TransformError> {
    let raw = expr_to_json_for_v2_condition(filter).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "finalize.filter must be a v2 condition",
        )
        .with_path("finalize.filter")
    })?;
    let cond = parse_v2_condition(&raw).map_err(|err| {
        TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid v2 condition: {}", err),
        )
        .with_path("finalize.filter")
    })?;
    let base_out = JsonValue::Array(records.clone());
    let mut filtered = Vec::new();
    for (index, item) in records.iter().enumerate() {
        let ctx = V2EvalContext::new().with_item(V2EvalItem { value: item, index });
        let keep = eval_v2_condition(&cond, item, context, &base_out, "finalize.filter", &ctx)?;
        if keep {
            filtered.push(item.clone());
        }
    }
    *records = filtered;
    Ok(())
}

pub(super) fn apply_filter_traced(
    records: &mut Vec<JsonValue>,
    filter: &Expr,
    context: Option<&JsonValue>,
    collector: &mut TraceCollector,
) -> Result<(), TransformError> {
    let raw = expr_to_json_for_v2_condition(filter).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "finalize.filter must be a v2 condition",
        )
        .with_path("finalize.filter")
    })?;
    let cond = parse_v2_condition(&raw).map_err(|err| {
        TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid v2 condition: {}", err),
        )
        .with_path("finalize.filter")
    })?;
    let base_out = JsonValue::Array(records.clone());
    let before_count = records.len();
    let mut filtered = Vec::new();
    for (index, item) in records.iter().enumerate() {
        let ctx = V2EvalContext::new().with_item(V2EvalItem { value: item, index });
        let item_path = format!("finalize.filter[{}]", index);
        let keep =
            eval_v2_condition_traced(&cond, item, context, &base_out, &item_path, &ctx, collector)?;
        collector
            .emit(TraceEventKind::FinalizeFilter, TracePhase::Instant)
            .rule_path(&item_path)
            .input_path(canonical_item_path(""))
            .attr_index("item_index", index)
            .attr_bool("kept", keep)
            .input_value(item, collector.options(), Some("@item"))
            .finish_with_output(collector, &JsonValue::Bool(keep), None);
        if keep {
            filtered.push(item.clone());
        }
    }
    *records = filtered;
    collector
        .emit(TraceEventKind::FinalizeFilter, TracePhase::Instant)
        .rule_path("finalize.filter")
        .attr_count("input_count", before_count)
        .attr_count("output_count", records.len())
        .finish_with_output(collector, &JsonValue::Array(records.clone()), None);
    Ok(())
}
