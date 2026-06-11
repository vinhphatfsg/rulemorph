use super::*;

pub(super) fn apply_mappings_traced(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
) -> Result<JsonValue, TransformError> {
    let mut out = JsonValue::Object(Map::new());
    apply_mappings_into_traced(
        rule,
        &rule.mappings,
        record,
        context,
        &mut out,
        warnings,
        rule.version,
        "mappings",
        limits,
        base_v2_ctx,
        collector,
    )?;
    Ok(out)
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn apply_mappings_into_traced(
    rule: &RuleFile,
    mappings: &[Mapping],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &mut JsonValue,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_path: &str,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
) -> Result<(), TransformError> {
    for (index, mapping) in mappings.iter().enumerate() {
        let mapping_path = format!("{}[{}]", base_path, index);
        collector
            .start_span(TraceEventKind::MappingStart, TracePhase::Start)
            .rule_path(&mapping_path)
            .attr_index("mapping_index", index)
            .finish(collector);

        let applied = if mapping.when.is_some() {
            let when_path = format!("{}.when", mapping_path);
            collector
                .start_span(TraceEventKind::MappingWhenStart, TracePhase::Start)
                .rule_path(&when_path)
                .finish(collector);
            let flag = eval_when_traced(TracedMappingWhenInput {
                eval: MappingWhenInput {
                    mapping,
                    record,
                    context,
                    out,
                    mapping_path: &mapping_path,
                    warnings,
                    rule_version,
                    limits,
                    ctx: base_v2_ctx,
                },
                collector,
            });
            collector
                .end_span(TraceEventKind::MappingWhenEnd, TracePhase::End)
                .rule_path(&when_path)
                .finish_with_output(collector, &JsonValue::Bool(flag), None);
            flag
        } else {
            true
        };

        collector
            .emit(TraceEventKind::MappingDecision, TracePhase::Instant)
            .rule_path(&mapping_path)
            .attr_bool("applied", applied)
            .attr_enum("skip_reason", if applied { "none" } else { "when_false" })
            .finish(collector);

        if !applied {
            collector
                .end_span(TraceEventKind::MappingEnd, TracePhase::End)
                .rule_path(&mapping_path)
                .finish(collector);
            continue;
        }

        let value = match eval_mapping_traced(MappingTraceInput {
            eval: MappingEvalInput {
                rule,
                mapping,
                record,
                context,
                out,
                mapping_path: &mapping_path,
                version: rule_version,
                limits,
                base_v2_ctx: Some(base_v2_ctx),
                compiled_mapping: None,
            },
            collector,
        }) {
            Ok(value) => value,
            Err(error) => {
                collector
                    .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                    .rule_path(&mapping_path)
                    .finish(collector);
                return Err(error);
            }
        };

        if let Some(value) = value {
            if let Err(error) = set_path(out, &mapping.target, value.clone(), &mapping_path) {
                collector
                    .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                    .rule_path(&mapping_path)
                    .finish(collector);
                return Err(error);
            }
            let output_redaction_hint = mapping_output_redaction_hint(mapping);
            collector
                .emit(TraceEventKind::OutputWrite, TracePhase::Instant)
                .rule_path(format!("{}.target", mapping_path))
                .output_path(canonical_output_path(&mapping.target))
                .attr_path("target_path", canonical_output_path(&mapping.target))
                .finish_with_output(collector, &value, Some(&output_redaction_hint));
        }

        collector
            .end_span(TraceEventKind::MappingEnd, TracePhase::End)
            .rule_path(&mapping_path)
            .finish(collector);
    }
    Ok(())
}

fn mapping_output_redaction_hint(mapping: &Mapping) -> String {
    let mut hint = mapping.target.clone();
    if let Some(source) = &mapping.source {
        hint.push(' ');
        hint.push_str(source);
    }
    if let Some(expr) = &mapping.expr {
        collect_expr_redaction_hints(expr, &mut hint);
    }
    hint
}

fn collect_expr_redaction_hints(expr: &Expr, hint: &mut String) {
    match expr {
        Expr::Ref(expr_ref) => {
            hint.push(' ');
            hint.push_str(&expr_ref.ref_path);
        }
        Expr::Op(expr_op) => {
            for arg in &expr_op.args {
                collect_expr_redaction_hints(arg, hint);
            }
        }
        Expr::Chain(expr_chain) => {
            for part in &expr_chain.chain {
                collect_expr_redaction_hints(part, hint);
            }
        }
        Expr::Literal(value) => collect_json_redaction_hints(value, hint),
    }
}

fn collect_json_redaction_hints(value: &JsonValue, hint: &mut String) {
    match value {
        JsonValue::String(value) if value.starts_with('@') => {
            hint.push(' ');
            hint.push_str(value);
        }
        JsonValue::Array(values) => {
            for value in values {
                collect_json_redaction_hints(value, hint);
            }
        }
        JsonValue::Object(values) => {
            for value in values.values() {
                collect_json_redaction_hints(value, hint);
            }
        }
        _ => {}
    }
}
