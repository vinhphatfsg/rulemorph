use super::super::*;

mod args;
mod collection;
mod lazy;

use args::{
    emit_arg_eval, eval_array_arg_traced, eval_eager_op_traced, eval_expr_at_index_traced,
    v1_operator_has_scoped_expr_args,
};
use collection::{eval_array_fold_traced, eval_array_map_traced, eval_array_reduce_traced};
use lazy::{eval_bool_and_or_traced, eval_coalesce_traced};

pub(in crate::transform) fn eval_expr_traced(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    collector
        .start_span(TraceEventKind::ExprStart, TracePhase::Start)
        .rule_path(base_path)
        .finish(collector);

    let result = match expr {
        Expr::Literal(value) => {
            let value = EvalValue::Value(value.clone());
            collector
                .emit(TraceEventKind::LiteralEval, TracePhase::Instant)
                .rule_path(base_path)
                .finish_with_eval_output(collector, &value, None);
            Ok(value)
        }
        Expr::Ref(expr_ref) => {
            let value = eval_ref(expr_ref, record, context, out, base_path, locals);
            if let Ok(value) = &value {
                let path_hint = ref_read_path_hint(base_path, &expr_ref.ref_path);
                collector
                    .emit(TraceEventKind::RefRead, TracePhase::Instant)
                    .rule_path(base_path)
                    .input_path(canonical_ref_path(&expr_ref.ref_path))
                    .finish_with_eval_output(collector, value, path_hint);
            }
            value
        }
        Expr::Op(expr_op) => {
            collector
                .start_span(TraceEventKind::OpStart, TracePhase::Start)
                .rule_path(base_path)
                .operator(&expr_op.op)
                .finish(collector);
            let op_result = match expr_op.op.as_str() {
                "coalesce" => eval_coalesce_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                "and" => eval_bool_and_or_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    true,
                    locals,
                    collector,
                ),
                "or" => eval_bool_and_or_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    false,
                    locals,
                    collector,
                ),
                "map" => eval_array_map_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                "reduce" => eval_array_reduce_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                "fold" => eval_array_fold_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                op if v1_operator_has_scoped_expr_args(op) => {
                    eval_op(expr_op, record, context, out, base_path, None, locals)
                }
                _ => eval_eager_op_traced(
                    expr_op, record, context, out, base_path, locals, collector,
                ),
            };

            match op_result {
                Ok(value) => {
                    collector
                        .end_span(TraceEventKind::OpEnd, TracePhase::End)
                        .rule_path(base_path)
                        .operator(&expr_op.op)
                        .finish_with_eval_output(collector, &value, None);
                    Ok(value)
                }
                Err(error) => {
                    collector
                        .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                        .rule_path(base_path)
                        .operator(&expr_op.op)
                        .finish(collector);
                    Err(error)
                }
            }
        }
        Expr::Chain(expr_chain) => eval_chain(expr_chain, record, context, out, base_path, locals),
    };

    match &result {
        Ok(value) => {
            collector
                .end_span(TraceEventKind::ExprEnd, TracePhase::End)
                .rule_path(base_path)
                .finish_with_eval_output(collector, value, None);
        }
        Err(_) => {
            collector
                .error_span(TraceEventKind::Error, "EXPR_ERROR", "expression failed")
                .rule_path(base_path)
                .finish(collector);
        }
    }

    result
}

fn ref_read_path_hint<'a>(base_path: &str, ref_path: &'a str) -> Option<&'a str> {
    if base_path.starts_with("defs.") && is_custom_body_local_ref(ref_path) {
        None
    } else {
        Some(ref_path)
    }
}

fn is_custom_body_local_ref(ref_path: &str) -> bool {
    matches!(
        ref_path,
        "input" | "out" | "local" | "item" | "acc" | "pipe"
    ) || ref_path.starts_with("input.")
        || ref_path.starts_with("input[")
        || ref_path.starts_with("out.")
        || ref_path.starts_with("out[")
        || ref_path.starts_with("local.")
        || ref_path.starts_with("local[")
        || ref_path.starts_with("item.")
        || ref_path.starts_with("item[")
        || ref_path.starts_with("acc.")
        || ref_path.starts_with("acc[")
        || ref_path.starts_with("pipe.")
        || ref_path.starts_with("pipe[")
}
