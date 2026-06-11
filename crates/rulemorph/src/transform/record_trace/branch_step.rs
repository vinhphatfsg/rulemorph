use super::*;
use crate::model::V2Branch;

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn apply_branch_step_traced(
    branch: &V2Branch,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    out: &mut JsonValue,
    rule_version: u8,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
    base_path: &str,
) -> Result<TracedStepOutcome, TransformError> {
    let branch_path = format!("{}.branch", base_path);
    let take = eval_when_expr_with_v2_context(
        &branch.when,
        record,
        context,
        out,
        &format!("{}.when", branch_path),
        rule_version,
        limits,
        base_v2_ctx,
    )?;
    collector
        .emit(TraceEventKind::BranchEval, TracePhase::Instant)
        .rule_path(format!("{}.when", branch_path))
        .finish_with_output(collector, &JsonValue::Bool(take), None);
    let (target, target_field) = if take {
        (Some(branch.then.as_str()), "then")
    } else {
        (branch.r#else.as_deref(), "else")
    };
    if let Some(target) = target {
        collector
            .start_span(TraceEventKind::BranchTaken, TracePhase::Start)
            .rule_path(&branch_path)
            .attr_enum("selected_branch", target_field)
            .finish(collector);
        let branch_path_guard = match branch_context.enter(base_dir, target) {
            Ok(guard) => guard,
            Err(err) => {
                collector
                    .error_span(TraceEventKind::Error, "BRANCH_ERROR", "branch failed")
                    .rule_path(&branch_path)
                    .finish(collector);
                return Err(err.with_path(format!("{}.{}", branch_path, target_field)));
            }
        };
        let branch_result = (|| {
            let (branch_rule, branch_base_dir) =
                load_rule_from_path(base_dir, target, branch_context.allowed_root())
                    .map_err(|err| err.with_path(format!("{}.{}", branch_path, target_field)))?;
            let branch_input = out.clone();
            transform_record_with_warnings_inner_traced(
                &branch_rule,
                &branch_input,
                context,
                Some(&branch_base_dir),
                branch_context,
                limits,
                collector,
            )
        })();
        branch_context.exit(branch_path_guard);
        let (branch_output, branch_warnings) = match branch_result {
            Ok(output) => output,
            Err(error) => {
                collector
                    .error_span(TraceEventKind::Error, "BRANCH_ERROR", "branch failed")
                    .rule_path(&branch_path)
                    .finish(collector);
                return Err(error);
            }
        };
        warnings.extend(branch_warnings);
        let Some(branch_output) = branch_output else {
            collector
                .end_span(TraceEventKind::BranchTaken, TracePhase::End)
                .rule_path(&branch_path)
                .finish(collector);
            return Ok(TracedStepOutcome::DropRecord);
        };

        if branch.return_ {
            collector
                .end_span(TraceEventKind::BranchTaken, TracePhase::End)
                .rule_path(&branch_path)
                .finish_with_output(collector, &branch_output, None);
            return Ok(TracedStepOutcome::Return(branch_output));
        }
        if let Err(error) = merge_branch_output(out, &branch_output, &branch_path) {
            collector
                .error_span(TraceEventKind::Error, "BRANCH_ERROR", "branch failed")
                .rule_path(&branch_path)
                .finish(collector);
            return Err(error);
        }
        collector
            .emit(TraceEventKind::BranchMerge, TracePhase::Instant)
            .rule_path(&branch_path)
            .finish_with_output(collector, out, None);
        collector
            .end_span(TraceEventKind::BranchTaken, TracePhase::End)
            .rule_path(&branch_path)
            .finish(collector);
    }
    Ok(TracedStepOutcome::Continue)
}
