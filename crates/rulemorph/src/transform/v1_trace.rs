use super::*;

mod args;
mod collection;

use args::{
    emit_arg_eval, eval_array_arg_traced, eval_eager_op_traced, eval_expr_at_index_traced,
    v1_operator_has_scoped_expr_args,
};
use collection::{eval_array_fold_traced, eval_array_map_traced, eval_array_reduce_traced};

pub(super) fn eval_expr_traced(
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
                collector
                    .emit(TraceEventKind::RefRead, TracePhase::Instant)
                    .rule_path(base_path)
                    .input_path(canonical_ref_path(&expr_ref.ref_path))
                    .finish_with_eval_output(collector, value, Some(&expr_ref.ref_path));
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

#[allow(clippy::too_many_arguments)]
fn eval_coalesce_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len == 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must be a non-empty array",
        )
        .with_path(format!("{}.args", base_path)));
    }

    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index_traced(
            index, args, injected, record, context, out, base_path, locals, collector,
        )?;
        emit_arg_eval(collector, &arg_path, index, &value);
        match value {
            EvalValue::Missing => continue,
            EvalValue::Value(value) => {
                if value.is_null() {
                    continue;
                }
                return Ok(EvalValue::Value(value));
            }
        }
    }
    Ok(EvalValue::Missing)
}

#[allow(clippy::too_many_arguments)]
fn eval_bool_and_or_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    is_and: bool,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut saw_missing = false;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index_traced(
            index, args, injected, record, context, out, base_path, locals, collector,
        )?;
        emit_arg_eval(collector, &arg_path, index, &value);
        match value {
            EvalValue::Missing => {
                saw_missing = true;
                continue;
            }
            EvalValue::Value(value) => {
                let flag = value_as_bool(&value, &arg_path)?;
                if is_and {
                    if !flag {
                        return Ok(EvalValue::Value(JsonValue::Bool(false)));
                    }
                } else if flag {
                    return Ok(EvalValue::Value(JsonValue::Bool(true)));
                }
            }
        }
    }

    if saw_missing {
        Ok(EvalValue::Missing)
    } else {
        Ok(EvalValue::Value(JsonValue::Bool(is_and)))
    }
}
