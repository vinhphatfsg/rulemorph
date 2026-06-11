use super::*;

mod map_step;
mod step;

use map_step::eval_v2_map_step_traced;
use step::eval_v2_step_traced;

pub(in crate::transform) fn eval_v2_pipe_traced<'a>(
    pipe: &V2Pipe,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    base_path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let _eval_scope = ctx.enter_eval_scope();
    collector
        .start_span(TraceEventKind::ExprStart, TracePhase::Start)
        .rule_path(base_path)
        .finish(collector);

    let first_path = format!("{}[0]", base_path);
    let literal_start_call = match crate::transform::parse_known_custom_call_literal_start(
        &pipe.start,
        ctx,
        &first_path,
    ) {
        Ok(call) => call,
        Err(error) => {
            collector
                .error_span(TraceEventKind::Error, "EXPR_ERROR", "expression failed")
                .rule_path(base_path)
                .finish(collector);
            return Err(error);
        }
    };
    let mut current = if let Some(call) = literal_start_call {
        let current = ctx
            .get_pipe_value()
            .cloned()
            .unwrap_or(V2EvalValue::Missing);
        emit_v2_start_trace(&V2Start::PipeValue, &current, base_path, collector);
        let step_ctx = ctx.clone().with_pipe_value(current.clone());
        let step = V2Step::CustomCall(call);
        let (current, _) = match eval_v2_step_traced(
            &step,
            current,
            record,
            context,
            out,
            &first_path,
            &step_ctx,
            collector,
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
        current
    } else {
        match eval_v2_start(&pipe.start, record, context, out, base_path, ctx) {
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

pub(in crate::transform) fn eval_v2_expr_traced<'a>(
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
        V2Start::PipeValue | V2Start::ImplicitPipeValue | V2Start::V1Expr(_) => {
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
        V2Ref::Pipe(path) => Some(if path.is_empty() {
            "$".to_string()
        } else if path.starts_with('[') {
            format!("${}", path)
        } else {
            format!("$.{}", path)
        }),
        V2Ref::Item(path) => Some(canonical_item_path(path)),
        V2Ref::Acc(path) => Some(canonical_acc_path(path)),
        V2Ref::Local(_) => None,
    }
}
