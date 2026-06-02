use super::*;

pub(super) fn apply_filter(
    rule: &RuleFile,
    records: &mut Vec<JsonValue>,
    filter: &Expr,
    context: Option<&JsonValue>,
    limits: EvalLimits,
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
        let ctx = V2EvalContext::new()
            .with_limits(limits)
            .with_rule(rule)
            .with_item(V2EvalItem { value: item, index });
        let keep = eval_v2_condition(&cond, item, context, &base_out, "finalize.filter", &ctx)?;
        if keep {
            filtered.push(item.clone());
        }
    }
    *records = filtered;
    Ok(())
}

pub(super) fn apply_filter_traced(
    rule: &RuleFile,
    records: &mut Vec<JsonValue>,
    filter: &Expr,
    context: Option<&JsonValue>,
    limits: EvalLimits,
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
        let ctx = V2EvalContext::new()
            .with_limits(limits)
            .with_rule(rule)
            .with_item(V2EvalItem { value: item, index });
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::TransformTraceOptions;
    use serde_json::json;

    #[test]
    fn filter_custom_op_calls_share_eval_limit_across_comparison_args() {
        let rule = crate::parse_rule_file(
            r#"
version: 2
input:
  format: json
  json: {}
defs:
  id:
    input: int
    returns: int
    expr: "$"
mappings: []
finalize:
  filter:
    eq:
      - ["@item.a", { map: [id] }, len]
      - ["@item.b", { map: [id] }, len]
"#,
        )
        .expect("rule parses");
        let filter = rule
            .finalize
            .as_ref()
            .and_then(|finalize| finalize.filter.as_ref())
            .expect("filter exists");
        let mut records = vec![json!({ "a": [1, 2], "b": [3, 4] })];
        let limits = EvalLimits {
            max_custom_op_calls_per_record: 3,
            ..EvalLimits::default()
        };

        let err = apply_filter(&rule, &mut records, filter, None, limits)
            .expect_err("calls across both comparison args share one eval limit");

        assert!(
            err.message
                .contains("custom op calls per record exceed configured limit")
        );
    }

    #[test]
    fn traced_filter_custom_op_calls_share_eval_limit_within_expression() {
        let rule = crate::parse_rule_file(
            r#"
version: 2
input:
  format: json
  json: {}
defs:
  id:
    input: int
    returns: int
    expr: "$"
mappings: []
finalize:
  filter:
    gt:
      - ["@item.values", { map: [id] }, len]
      - 0
"#,
        )
        .expect("rule parses");
        let filter = rule
            .finalize
            .as_ref()
            .and_then(|finalize| finalize.filter.as_ref())
            .expect("filter exists");
        let mut records = vec![json!({ "values": [1, 2, 3, 4] })];
        let limits = EvalLimits {
            max_custom_op_calls_per_record: 3,
            ..EvalLimits::default()
        };
        let mut collector = TraceCollector::new(TransformTraceOptions::metadata_only());

        let err = apply_filter_traced(&rule, &mut records, filter, None, limits, &mut collector)
            .expect_err("traced filter enforces custom op calls within one expression");

        assert!(
            err.message
                .contains("custom op calls per record exceed configured limit")
        );
    }
}
