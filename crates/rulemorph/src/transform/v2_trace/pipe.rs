use super::*;
use crate::v2_operator::{V2OperatorTrace, operator};

pub(in crate::transform) fn eval_v2_pipe_traced<'a>(
    pipe: &V2Pipe,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    base_path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    collector
        .start_span(TraceEventKind::ExprStart, TracePhase::Start)
        .rule_path(base_path)
        .finish(collector);

    let mut current = match eval_v2_start(&pipe.start, record, context, out, base_path, ctx) {
        Ok(value) => {
            emit_v2_start_trace(&pipe.start, &value, base_path, collector);
            value
        }
        Err(error) => {
            collector
                .error_span(TraceEventKind::Error, "EXPR_ERROR", "expression failed")
                .rule_path(base_path)
                .finish(collector);
            return Err(error);
        }
    };
    let mut current_ctx = ctx.clone();

    for (step_index, step) in pipe.steps.iter().enumerate() {
        let step_path = format!("{}[{}]", base_path, step_index + 1);
        let step_ctx = current_ctx.clone().with_pipe_value(current.clone());
        let (next, next_ctx) = match eval_v2_step_traced(
            step, current, record, context, out, &step_path, &step_ctx, collector,
        ) {
            Ok(result) => result,
            Err(error) => {
                collector
                    .error_span(TraceEventKind::Error, "EXPR_ERROR", "expression failed")
                    .rule_path(base_path)
                    .finish(collector);
                return Err(error);
            }
        };
        current = next;
        current_ctx = next_ctx;
    }

    collector
        .end_span(TraceEventKind::ExprEnd, TracePhase::End)
        .rule_path(base_path)
        .finish_with_v2_eval_output(collector, &current, None);
    Ok(current)
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_step_traced<'a>(
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
        V2Step::Map(map) => {
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
                                .error_span(
                                    TraceEventKind::Error,
                                    "COLLECTION_ERROR",
                                    "item failed",
                                )
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
                    results.push(value);
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

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_v2_expr_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    match expr {
        crate::v2_model::V2Expr::Pipe(pipe) => {
            eval_v2_pipe_traced(pipe, record, context, out, path, ctx, collector)
        }
        crate::v2_model::V2Expr::V1Fallback(_) => {
            eval_v2_expr(expr, record, context, out, path, ctx)
        }
    }
}

fn emit_v2_start_trace(
    start: &V2Start,
    value: &V2EvalValue,
    path: &str,
    collector: &mut TraceCollector,
) {
    match start {
        V2Start::Ref(v2_ref) => {
            let mut event = collector
                .emit(TraceEventKind::RefRead, TracePhase::Instant)
                .rule_path(path);
            if let Some(input_path) = canonical_v2_ref_path(v2_ref) {
                event = event.input_path(input_path);
            }
            event.finish_with_v2_eval_output(collector, value, None);
        }
        V2Start::Literal(_) => {
            collector
                .emit(TraceEventKind::LiteralEval, TracePhase::Instant)
                .rule_path(path)
                .finish_with_v2_eval_output(collector, value, None);
        }
        V2Start::PipeValue | V2Start::V1Expr(_) => {
            collector
                .emit(TraceEventKind::ChainStep, TracePhase::Instant)
                .rule_path(path)
                .finish_with_v2_eval_output(collector, value, None);
        }
    }
}

fn canonical_v2_ref_path(v2_ref: &V2Ref) -> Option<String> {
    match v2_ref {
        V2Ref::Input(path) => Some(canonical_input_path(path)),
        V2Ref::Context(path) => Some(canonical_context_path(path)),
        V2Ref::Out(path) => Some(canonical_out_path(path)),
        V2Ref::Item(path) => Some(canonical_item_path(path)),
        V2Ref::Acc(path) => Some(canonical_acc_path(path)),
        V2Ref::Local(_) => None,
    }
}
