use super::*;

mod branch_step;
mod mapping;
mod steps;

use mapping::apply_mappings_traced;
use steps::{TracedStepOutcome, apply_steps_traced};

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
