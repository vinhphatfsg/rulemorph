use super::*;

pub(super) fn eval_custom_op_body(
    rule: &RuleFile,
    name: &str,
    def: &CustomOpDef,
    input: &JsonValue,
    path: &str,
    limits: EvalLimits,
    caller_ctx: &V2EvalContext<'_>,
) -> Result<JsonValue, TransformError> {
    if let Some(expr) = &def.expr {
        let expr_path = format!("defs.{}.expr", name);
        let value = expr_to_json_for_v2_pipe(expr).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "custom op expr must be a v2 pipe",
            )
            .with_path(&expr_path)
        })?;
        let pipe = parse_v2_pipe_from_value(&value).map_err(|err| {
            TransformError::new(TransformErrorKind::ExprError, err.to_string())
                .with_path(&expr_path)
        })?;
        let empty_out = JsonValue::Object(Map::new());
        let body_ctx = V2EvalContext::new()
            .with_limits(limits)
            .with_rule(rule)
            .with_pipe_value(V2EvalValue::Value(input.clone()))
            .with_custom_op_depth(caller_ctx.custom_op_depth() + 1)
            .with_custom_op_counter_from(caller_ctx);
        return match eval_v2_pipe(&pipe, input, None, &empty_out, &expr_path, &body_ctx)? {
            V2EvalValue::Value(value) => Ok(value),
            V2EvalValue::Missing => Err(TransformError::new(
                TransformErrorKind::ExprError,
                "custom op output contract mismatch: output is missing",
            )
            .with_path(path)),
        };
    }

    if let Some(mappings) = &def.mappings {
        let mut output = JsonValue::Object(Map::new());
        let body_ctx = V2EvalContext::new()
            .with_limits(limits)
            .with_rule(rule)
            .with_pipe_value(V2EvalValue::Value(input.clone()))
            .with_custom_op_depth(caller_ctx.custom_op_depth() + 1)
            .with_custom_op_counter_from(caller_ctx);
        for (index, mapping) in mappings.iter().enumerate() {
            let mapping_path = format!("defs.{}.mappings[{}]", name, index);
            if let Some(when) = &mapping.when {
                let keep = eval_when_expr_with_v2_context(
                    when,
                    input,
                    None,
                    &output,
                    &mapping_path,
                    2,
                    limits,
                    &body_ctx,
                )?;
                if !keep {
                    continue;
                }
            }
            let value = eval_mapping_with_v2_context(MappingEvalInput {
                rule,
                mapping,
                record: input,
                context: None,
                out: &output,
                mapping_path: &mapping_path,
                version: 2,
                limits,
                base_v2_ctx: Some(&body_ctx),
                compiled_mapping: None,
            })?;
            if let Some(value) = value {
                set_path(&mut output, &mapping.target, value, &mapping_path)?;
            }
        }
        return Ok(output);
    }

    Err(TransformError::new(
        TransformErrorKind::ExprError,
        "custom op must define expr or mappings",
    )
    .with_path(path))
}

pub(super) fn eval_custom_op_body_traced(
    rule: &RuleFile,
    name: &str,
    def: &CustomOpDef,
    input: &JsonValue,
    input_redaction_hints: &CustomInputRedactionHints,
    path: &str,
    limits: EvalLimits,
    caller_ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
) -> Result<JsonValue, TransformError> {
    if let Some(expr) = &def.expr {
        let expr_path = format!("defs.{}.expr", name);
        let value = expr_to_json_for_v2_pipe(expr).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "custom op expr must be a v2 pipe",
            )
            .with_path(&expr_path)
        })?;
        let pipe = parse_v2_pipe_from_value(&value).map_err(|err| {
            TransformError::new(TransformErrorKind::ExprError, err.to_string())
                .with_path(&expr_path)
        })?;
        let empty_out = JsonValue::Object(Map::new());
        let body_ctx = V2EvalContext::new()
            .with_limits(limits)
            .with_rule(rule)
            .with_pipe_value(V2EvalValue::Value(input.clone()))
            .with_custom_op_depth(caller_ctx.custom_op_depth() + 1)
            .with_custom_op_counter_from(caller_ctx);
        return match eval_v2_pipe_traced(
            &pipe, input, None, &empty_out, &expr_path, &body_ctx, collector,
        )? {
            V2EvalValue::Value(value) => Ok(value),
            V2EvalValue::Missing => Err(TransformError::new(
                TransformErrorKind::ExprError,
                "custom op output contract mismatch: output is missing",
            )
            .with_path(path)),
        };
    }

    if let Some(mappings) = &def.mappings {
        let mut output = JsonValue::Object(Map::new());
        let body_ctx = V2EvalContext::new()
            .with_limits(limits)
            .with_rule(rule)
            .with_pipe_value(V2EvalValue::Value(input.clone()))
            .with_custom_op_depth(caller_ctx.custom_op_depth() + 1)
            .with_custom_op_counter_from(caller_ctx);
        for (index, mapping) in mappings.iter().enumerate() {
            let mapping_path = format!("defs.{}.mappings[{}]", name, index);
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
                let keep = match eval_when_expr_traced_with_v2_context(
                    mapping.when.as_ref().expect("checked above"),
                    input,
                    None,
                    &output,
                    &when_path,
                    2,
                    limits,
                    &body_ctx,
                    collector,
                ) {
                    Ok(keep) => keep,
                    Err(mut error) => {
                        collector
                            .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                            .rule_path(&when_path)
                            .finish(collector);
                        collector
                            .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                            .rule_path(&mapping_path)
                            .finish(collector);
                        error.path = normalize_custom_body_when_error_path(
                            error.path,
                            &mapping_path,
                            &when_path,
                        );
                        return Err(error);
                    }
                };
                collector
                    .end_span(TraceEventKind::MappingWhenEnd, TracePhase::End)
                    .rule_path(&when_path)
                    .finish_with_output(collector, &JsonValue::Bool(keep), None);
                keep
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

            let value = match eval_custom_mapping_traced(CustomMappingTraceInput {
                rule,
                mapping,
                input,
                output: &output,
                mapping_path: &mapping_path,
                limits,
                body_ctx: &body_ctx,
                input_redaction_hints,
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
                if let Err(error) =
                    set_path(&mut output, &mapping.target, value.clone(), &mapping_path)
                {
                    collector
                        .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                        .rule_path(&mapping_path)
                        .finish(collector);
                    return Err(error);
                }
                let output_redaction_hint =
                    mapping_output_redaction_hint(mapping, input_redaction_hints);
                collector
                    .emit(TraceEventKind::OutputWrite, TracePhase::Instant)
                    .rule_path(format!("{}.target", mapping_path))
                    .output_path(canonical_output_path(&mapping.target))
                    .attr_path("target_path", canonical_output_path(&mapping.target))
                    .finish_with_output(collector, &value, output_redaction_hint.as_deref());
            }

            collector
                .end_span(TraceEventKind::MappingEnd, TracePhase::End)
                .rule_path(&mapping_path)
                .finish(collector);
        }
        return Ok(output);
    }

    Err(TransformError::new(
        TransformErrorKind::ExprError,
        "custom op must define expr or mappings",
    )
    .with_path(path))
}

pub(super) struct CustomMappingTraceInput<'a, 'ctx, 'collector> {
    pub(super) rule: &'a RuleFile,
    pub(super) mapping: &'a Mapping,
    pub(super) input: &'a JsonValue,
    pub(super) output: &'a JsonValue,
    pub(super) mapping_path: &'a str,
    pub(super) limits: EvalLimits,
    pub(super) body_ctx: &'a V2EvalContext<'ctx>,
    pub(super) input_redaction_hints: &'a CustomInputRedactionHints,
    pub(super) collector: &'collector mut TraceCollector,
}

pub(super) fn eval_custom_mapping_traced(
    input: CustomMappingTraceInput<'_, '_, '_>,
) -> Result<Option<JsonValue>, TransformError> {
    let CustomMappingTraceInput {
        rule,
        mapping,
        input,
        output,
        mapping_path,
        limits,
        body_ctx,
        input_redaction_hints,
        collector,
    } = input;
    let eval = MappingEvalInput {
        rule,
        mapping,
        record: input,
        context: None,
        out: output,
        mapping_path,
        version: 2,
        limits,
        base_v2_ctx: Some(body_ctx),
        compiled_mapping: None,
    };
    let Some(source) = &mapping.source else {
        return eval_mapping_traced(MappingTraceInput { eval, collector });
    };

    match custom_body_path_redaction_hint_override(source, input_redaction_hints) {
        Some(RedactionHintOverride::Hint(path_hint)) => {
            eval_mapping_traced_with_source_redaction_hint(
                MappingTraceInput { eval, collector },
                Some(&path_hint),
            )
        }
        Some(RedactionHintOverride::Unknown) => eval_mapping_traced_with_source_redaction_hint(
            MappingTraceInput { eval, collector },
            None,
        ),
        None => eval_mapping_traced(MappingTraceInput { eval, collector }),
    }
}

pub(super) fn synthesize_mappings_return(mappings: &[Mapping]) -> RuleType {
    let mut fields = BTreeMap::new();
    for mapping in mappings {
        let Ok(tokens) = parse_path(&mapping.target) else {
            continue;
        };
        insert_return_path(&mut fields, &tokens, mapping_may_be_absent(mapping));
    }
    RuleType {
        kind: RuleTypeKind::Object(fields),
        nullable: false,
    }
}
