use super::branch_step::apply_branch_step_traced;
use super::mapping::apply_mappings_into_traced;
use super::*;

pub(super) enum TracedStepOutcome {
    Continue,
    DropRecord,
    Return(JsonValue),
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_steps_traced(
    rule: &RuleFile,
    steps: &[V2RuleStep],
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
) -> Result<Option<JsonValue>, TransformError> {
    let mut out = JsonValue::Object(Map::new());

    for (step_index, step) in steps.iter().enumerate() {
        let base_path = format!("steps[{}]", step_index);
        collector
            .start_span(TraceEventKind::StepStart, TracePhase::Start)
            .rule_path(&base_path)
            .attr_index("step_index", step_index)
            .finish(collector);

        let step_result = (|| -> Result<TracedStepOutcome, TransformError> {
            if let Some(mappings) = &step.mappings {
                apply_mappings_into_traced(
                    rule,
                    mappings,
                    record,
                    context,
                    &mut out,
                    warnings,
                    rule_version,
                    &format!("{}.mappings", base_path),
                    limits,
                    base_v2_ctx,
                    collector,
                )?;
                return Ok(TracedStepOutcome::Continue);
            }

            if let Some(expr) = &step.record_when {
                let when_path = format!("{}.record_when", base_path);
                collector
                    .start_span(TraceEventKind::RecordWhenStart, TracePhase::Start)
                    .rule_path(&when_path)
                    .finish(collector);
                let keep = match eval_when_expr_traced_with_v2_context(
                    expr,
                    record,
                    context,
                    &out,
                    &when_path,
                    rule_version,
                    limits,
                    base_v2_ctx,
                    collector,
                ) {
                    Ok(keep) => keep,
                    Err(error) => {
                        collector
                            .error_span(
                                TraceEventKind::Error,
                                "RECORD_WHEN_ERROR",
                                "record_when failed",
                            )
                            .rule_path(&when_path)
                            .finish(collector);
                        return Err(error);
                    }
                };
                collector
                    .end_span(TraceEventKind::RecordWhenEnd, TracePhase::End)
                    .rule_path(&when_path)
                    .finish_with_output(collector, &JsonValue::Bool(keep), None);
                collector
                    .emit(TraceEventKind::RecordDecision, TracePhase::Instant)
                    .rule_path(&when_path)
                    .attr_bool("kept", keep)
                    .finish(collector);
                if !keep {
                    return Ok(TracedStepOutcome::DropRecord);
                }
                return Ok(TracedStepOutcome::Continue);
            }

            if let Some(asserts) = &step.asserts {
                for (assert_index, assert) in asserts.iter().enumerate() {
                    let assert_path = format!("{}.asserts[{}]", base_path, assert_index);
                    let ok = eval_when_expr_with_v2_context(
                        &assert.when,
                        record,
                        context,
                        &out,
                        &format!("{}.when", assert_path),
                        rule_version,
                        limits,
                        base_v2_ctx,
                    )?;
                    collector
                        .emit(TraceEventKind::AssertEval, TracePhase::Instant)
                        .rule_path(&assert_path)
                        .attr_index("assert_index", assert_index)
                        .finish_with_output(collector, &JsonValue::Bool(ok), None);
                    if !ok {
                        return Err(TransformError::new(
                            TransformErrorKind::AssertionFailed,
                            format!(
                                "assert failed: {}: {}",
                                assert.error.code, assert.error.message
                            ),
                        )
                        .with_path(assert_path));
                    }
                }
                return Ok(TracedStepOutcome::Continue);
            }

            if let Some(branch) = &step.branch {
                return apply_branch_step_traced(
                    branch,
                    record,
                    context,
                    warnings,
                    &mut out,
                    rule_version,
                    base_dir,
                    branch_context,
                    limits,
                    base_v2_ctx,
                    collector,
                    &base_path,
                );
            }

            Ok(TracedStepOutcome::Continue)
        })();

        match step_result {
            Ok(TracedStepOutcome::DropRecord) => {
                collector
                    .end_span(TraceEventKind::StepStart, TracePhase::End)
                    .rule_path(&base_path)
                    .finish(collector);
                return Ok(None);
            }
            Ok(TracedStepOutcome::Return(value)) => {
                collector
                    .end_span(TraceEventKind::StepStart, TracePhase::End)
                    .rule_path(&base_path)
                    .finish_with_output(collector, &value, None);
                return Ok(Some(value));
            }
            Ok(TracedStepOutcome::Continue) => {
                collector
                    .end_span(TraceEventKind::StepStart, TracePhase::End)
                    .rule_path(&base_path)
                    .finish(collector);
            }
            Err(error) => {
                collector
                    .error_span(TraceEventKind::Error, "STEP_ERROR", "step failed")
                    .rule_path(&base_path)
                    .finish(collector);
                return Err(error);
            }
        }
    }

    Ok(Some(out))
}
