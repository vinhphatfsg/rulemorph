use super::*;

mod branch_step;
mod mapping;

use branch_step::apply_branch_step_traced;
use mapping::{apply_mappings_into_traced, apply_mappings_traced};

pub(super) fn apply_rule_to_record_traced(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
    collector: &mut TraceCollector,
) -> Result<Option<JsonValue>, TransformError> {
    if let Some(steps) = &rule.steps {
        return apply_steps_traced(
            steps,
            record,
            context,
            warnings,
            rule.version,
            base_dir,
            branch_context,
            collector,
        );
    }

    if rule.record_when.is_some() {
        collector
            .start_span(TraceEventKind::RecordWhenStart, TracePhase::Start)
            .rule_path("record_when")
            .finish(collector);
    }
    let keep = eval_record_when_traced(rule, record, context, warnings, collector);
    if rule.record_when.is_some() {
        collector
            .end_span(TraceEventKind::RecordWhenEnd, TracePhase::End)
            .rule_path("record_when")
            .finish_with_output(collector, &JsonValue::Bool(keep), None);
    }
    collector
        .emit(TraceEventKind::RecordDecision, TracePhase::Instant)
        .attr_bool("kept", keep)
        .finish(collector);
    if !keep {
        return Ok(None);
    }

    let output = apply_mappings_traced(rule, record, context, warnings, collector)?;
    Ok(Some(output))
}

#[allow(clippy::too_many_arguments)]
fn transform_record_with_warnings_inner_traced(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
    collector: &mut TraceCollector,
) -> Result<(Option<JsonValue>, Vec<TransformWarning>), TransformError> {
    let mut warnings = Vec::new();
    let output = apply_rule_to_record_traced(
        rule,
        record,
        context,
        &mut warnings,
        base_dir,
        branch_context,
        collector,
    )?;
    let Some(output) = output else {
        return Ok((None, warnings));
    };

    if let Some(finalize) = &rule.finalize {
        let array = JsonValue::Array(vec![output]);
        collector
            .start_span(TraceEventKind::FinalizeStart, TracePhase::Start)
            .rule_path("finalize")
            .finish_with_output(collector, &array, None);
        match apply_finalize_traced(finalize, array, context, collector) {
            Ok(finalized) => {
                collector
                    .end_span(TraceEventKind::FinalizeEnd, TracePhase::End)
                    .rule_path("finalize")
                    .finish(collector);
                return Ok((Some(finalized), warnings));
            }
            Err(error) => {
                collector
                    .error_span(TraceEventKind::Error, "FINALIZE_ERROR", "finalize failed")
                    .rule_path("finalize")
                    .finish(collector);
                return Err(error);
            }
        }
    }

    Ok((Some(output), warnings))
}

enum TracedStepOutcome {
    Continue,
    DropRecord,
    Return(JsonValue),
}

#[allow(clippy::too_many_arguments)]
fn apply_steps_traced(
    steps: &[V2RuleStep],
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
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
                    mappings,
                    record,
                    context,
                    &mut out,
                    warnings,
                    rule_version,
                    &format!("{}.mappings", base_path),
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
                let keep = match eval_when_expr_traced(
                    expr,
                    record,
                    context,
                    &out,
                    &when_path,
                    rule_version,
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
                    let ok = eval_when_expr(
                        &assert.when,
                        record,
                        context,
                        &out,
                        &format!("{}.when", assert_path),
                        rule_version,
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
