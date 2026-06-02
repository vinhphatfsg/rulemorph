use super::*;
use crate::model::{CustomOpDef, RuleType, RuleTypeKind};
use crate::v2_operator::{V2OperatorTrace, operator};

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_v2_step_traced<'a>(
    step: &V2Step,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    step_path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<(V2EvalValue, V2EvalContext<'a>), TransformError> {
    match step {
        V2Step::Op(op)
            if !op.op.starts_with('@')
                && ctx
                    .rule()
                    .is_some_and(|rule| rule.defs.contains_key(&op.op)) =>
        {
            let def = ctx
                .rule()
                .and_then(|rule| rule.defs.get(&op.op))
                .expect("custom op exists after trace guard");
            let def_path = format!("defs.{}", op.op);
            let input_type = trace_type_summary(&def.input);
            let output_type = trace_output_type_summary(def);
            collector
                .start_span(TraceEventKind::OpStart, TracePhase::Start)
                .rule_path(step_path)
                .operator(&op.op)
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .attr_enum("kind", "custom_op")
                .attr_path("name", op.op.clone())
                .attr_path("def_path", def_path.clone())
                .attr_path("call_path", step_path)
                .attr_path("input_type", input_type.clone())
                .attr_path("output_type", output_type.clone())
                .attr_bool("with_adapter", false)
                .attr_bool("body_truncated", false)
                .attr_count("arg_count", op.args.len())
                .finish(collector);
            let output = match eval_custom_op_step_traced(
                op,
                pipe_value.clone(),
                record,
                context,
                out,
                step_path,
                ctx,
                collector,
            ) {
                Ok(Some(output)) => output,
                Ok(None) => {
                    eval_v2_op_step(op, pipe_value.clone(), record, context, out, step_path, ctx)?
                }
                Err(error) => {
                    collector
                        .error_span(TraceEventKind::OpError, "OP_ERROR", "custom op failed")
                        .rule_path(step_path)
                        .operator(&op.op)
                        .input_v2_eval_value(&pipe_value, collector.options(), None)
                        .attr_enum("kind", "custom_op")
                        .attr_path("name", op.op.clone())
                        .attr_path("def_path", def_path.clone())
                        .attr_path("call_path", step_path)
                        .attr_path("input_type", input_type.clone())
                        .attr_path("output_type", output_type.clone())
                        .attr_bool("with_adapter", false)
                        .finish(collector);
                    return Err(error);
                }
            };
            collector
                .end_span(TraceEventKind::OpEnd, TracePhase::End)
                .rule_path(step_path)
                .operator(&op.op)
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .attr_enum("kind", "custom_op")
                .attr_path("name", op.op.clone())
                .attr_path("def_path", def_path)
                .attr_path("call_path", step_path)
                .attr_path("input_type", input_type)
                .attr_path("output_type", output_type)
                .attr_bool("with_adapter", false)
                .attr_bool("body_truncated", false)
                .finish_with_v2_eval_output(collector, &output, None);
            Ok((output, ctx.clone()))
        }
        V2Step::Op(op) => {
            collector
                .start_span(TraceEventKind::OpStart, TracePhase::Start)
                .rule_path(step_path)
                .operator(&op.op)
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .attr_count("arg_count", op.args.len())
                .finish(collector);
            let operator_metadata = operator(&op.op);
            let trace = operator_metadata
                .map(|metadata| metadata.trace)
                .unwrap_or(V2OperatorTrace::EagerArgs);
            let result = match trace {
                V2OperatorTrace::ItemLevelCollection => eval_v2_collection_op_traced(
                    op,
                    pipe_value.clone(),
                    record,
                    context,
                    out,
                    step_path,
                    ctx,
                    collector,
                ),
                V2OperatorTrace::EagerArgs => eval_v2_eager_op_traced(
                    op,
                    pipe_value.clone(),
                    record,
                    context,
                    out,
                    step_path,
                    ctx,
                    operator_metadata,
                    collector,
                ),
                V2OperatorTrace::LazyShortCircuit => eval_v2_lazy_op_traced(
                    op,
                    pipe_value.clone(),
                    record,
                    context,
                    out,
                    step_path,
                    ctx,
                    collector,
                ),
                V2OperatorTrace::Delegated => {
                    eval_v2_op_step(op, pipe_value.clone(), record, context, out, step_path, ctx)
                }
            };
            let output = match result {
                Ok(output) => output,
                Err(error) => {
                    collector
                        .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                        .rule_path(step_path)
                        .operator(&op.op)
                        .input_v2_eval_value(&pipe_value, collector.options(), None)
                        .finish(collector);
                    return Err(error);
                }
            };
            collector
                .end_span(TraceEventKind::OpEnd, TracePhase::End)
                .rule_path(step_path)
                .operator(&op.op)
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .finish_with_v2_eval_output(collector, &output, None);
            Ok((output, ctx.clone()))
        }
        V2Step::CustomCall(call) => {
            let def = ctx.rule().and_then(|rule| rule.defs.get(&call.op));
            let def_path = format!("defs.{}", call.op);
            let input_type = def
                .map(|def| trace_type_summary(&def.input))
                .unwrap_or_else(|| "unknown".to_string());
            let output_type = def
                .map(trace_output_type_summary)
                .unwrap_or_else(|| "unknown".to_string());
            collector
                .start_span(TraceEventKind::OpStart, TracePhase::Start)
                .rule_path(step_path)
                .operator(&call.op)
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .attr_enum("kind", "custom_op")
                .attr_path("name", call.op.clone())
                .attr_path("def_path", def_path.clone())
                .attr_path("call_path", step_path)
                .attr_path("input_type", input_type.clone())
                .attr_path("output_type", output_type.clone())
                .attr_bool("with_adapter", true)
                .attr_bool("body_truncated", false)
                .attr_count("arg_count", call.with.as_ref().map_or(0, Vec::len))
                .finish(collector);
            let output = match eval_custom_call_step_traced(
                call,
                pipe_value.clone(),
                record,
                context,
                out,
                step_path,
                ctx,
                collector,
            ) {
                Ok(output) => output,
                Err(error) => {
                    collector
                        .error_span(TraceEventKind::OpError, "OP_ERROR", "custom op failed")
                        .rule_path(step_path)
                        .operator(&call.op)
                        .input_v2_eval_value(&pipe_value, collector.options(), None)
                        .attr_enum("kind", "custom_op")
                        .attr_path("name", call.op.clone())
                        .attr_path("def_path", def_path.clone())
                        .attr_path("call_path", step_path)
                        .attr_path("input_type", input_type.clone())
                        .attr_path("output_type", output_type.clone())
                        .attr_bool("with_adapter", true)
                        .finish(collector);
                    return Err(error);
                }
            };
            collector
                .end_span(TraceEventKind::OpEnd, TracePhase::End)
                .rule_path(step_path)
                .operator(&call.op)
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .attr_enum("kind", "custom_op")
                .attr_path("name", call.op.clone())
                .attr_path("def_path", def_path)
                .attr_path("call_path", step_path)
                .attr_path("input_type", input_type)
                .attr_path("output_type", output_type)
                .attr_bool("with_adapter", true)
                .attr_bool("body_truncated", false)
                .finish_with_v2_eval_output(collector, &output, None);
            Ok((output, ctx.clone()))
        }
        V2Step::Map(map) => eval_v2_map_step_traced(
            map, pipe_value, record, context, out, step_path, ctx, collector,
        ),
        V2Step::Let(let_step) => {
            let new_ctx = eval_v2_let_step(
                let_step,
                pipe_value.clone(),
                record,
                context,
                out,
                step_path,
                ctx,
            )?;
            collector
                .emit(TraceEventKind::ChainStep, TracePhase::Instant)
                .rule_path(step_path)
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .finish(collector);
            let output = new_ctx.get_pipe_value().cloned().unwrap_or(pipe_value);
            Ok((output, new_ctx))
        }
        V2Step::If(if_step) => {
            let cond_ctx = ctx.clone().with_pipe_value(pipe_value.clone());
            let cond_path = format!("{}.cond", step_path);
            let cond = eval_v2_condition_traced(
                &if_step.cond,
                record,
                context,
                out,
                &cond_path,
                &cond_ctx,
                collector,
            )?;
            collector
                .emit(TraceEventKind::BranchEval, TracePhase::Instant)
                .rule_path(&cond_path)
                .finish_with_output(collector, &JsonValue::Bool(cond), None);
            if cond {
                collector
                    .start_span(TraceEventKind::BranchTaken, TracePhase::Start)
                    .rule_path(step_path)
                    .attr_enum("selected_branch", "then")
                    .finish(collector);
                let result = eval_v2_pipe_traced(
                    &if_step.then_branch,
                    record,
                    context,
                    out,
                    &format!("{}.then", step_path),
                    &cond_ctx,
                    collector,
                )?;
                collector
                    .end_span(TraceEventKind::BranchTaken, TracePhase::End)
                    .rule_path(step_path)
                    .finish_with_v2_eval_output(collector, &result, None);
                Ok((result, ctx.clone()))
            } else if let Some(else_branch) = &if_step.else_branch {
                collector
                    .start_span(TraceEventKind::BranchTaken, TracePhase::Start)
                    .rule_path(step_path)
                    .attr_enum("selected_branch", "else")
                    .finish(collector);
                let result = eval_v2_pipe_traced(
                    else_branch,
                    record,
                    context,
                    out,
                    &format!("{}.else", step_path),
                    &cond_ctx,
                    collector,
                )?;
                collector
                    .end_span(TraceEventKind::BranchTaken, TracePhase::End)
                    .rule_path(step_path)
                    .finish_with_v2_eval_output(collector, &result, None);
                Ok((result, ctx.clone()))
            } else {
                Ok((pipe_value, ctx.clone()))
            }
        }
        V2Step::Ref(v2_ref) => {
            let result = eval_v2_ref(v2_ref, record, context, out, step_path, ctx)?;
            let mut event = collector
                .emit(TraceEventKind::RefRead, TracePhase::Instant)
                .rule_path(step_path);
            if let Some(path) = canonical_v2_ref_path(v2_ref) {
                event = event.input_path(path);
            }
            event.finish_with_v2_eval_output(collector, &result, None);
            Ok((result, ctx.clone()))
        }
    }
}

fn trace_output_type_summary(def: &CustomOpDef) -> String {
    def.returns
        .as_ref()
        .map(trace_type_summary)
        .unwrap_or_else(|| "synthetic_object".to_string())
}

fn trace_type_summary(ty: &RuleType) -> String {
    let summary = trace_type_summary_inner(ty, 0);
    const MAX_TRACE_TYPE_SUMMARY: usize = 120;
    if summary.chars().count() <= MAX_TRACE_TYPE_SUMMARY {
        summary
    } else {
        format!(
            "{}...",
            summary
                .chars()
                .take(MAX_TRACE_TYPE_SUMMARY)
                .collect::<String>()
        )
    }
}

fn trace_type_summary_inner(ty: &RuleType, depth: usize) -> String {
    if depth >= 4 {
        return "...".to_string();
    }
    let base = match &ty.kind {
        RuleTypeKind::String => "string".to_string(),
        RuleTypeKind::Int => "int".to_string(),
        RuleTypeKind::Float => "float".to_string(),
        RuleTypeKind::Number => "number".to_string(),
        RuleTypeKind::Bool => "bool".to_string(),
        RuleTypeKind::Json => "json".to_string(),
        RuleTypeKind::Array(item) => {
            format!("[{}]", trace_type_summary_inner(item, depth + 1))
        }
        RuleTypeKind::Object(fields) => {
            let mut items = fields
                .iter()
                .take(4)
                .map(|(key, field)| {
                    let suffix = if field.optional { "?" } else { "" };
                    format!(
                        "{}{}: {}",
                        key,
                        suffix,
                        trace_type_summary_inner(&field.ty, depth + 1)
                    )
                })
                .collect::<Vec<_>>();
            if fields.len() > 4 {
                items.push("...".to_string());
            }
            format!("{{ {} }}", items.join(", "))
        }
    };
    if ty.nullable {
        format!("{}?", base)
    } else {
        base
    }
}
