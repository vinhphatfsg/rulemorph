use super::*;

pub(in crate::transform) fn emit_v2_arg_eval(
    collector: &mut TraceCollector,
    rule_path: &str,
    arg_index: usize,
    operator: &str,
    value: &V2EvalValue,
) {
    collector
        .emit(TraceEventKind::ArgEval, TracePhase::Instant)
        .rule_path(rule_path)
        .operator(operator)
        .attr_index("arg_index", arg_index)
        .finish_with_v2_eval_output(collector, value, None);
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(in crate::transform) fn eval_v2_lazy_op_traced<'a>(
    op: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    step_path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let step_ctx = ctx.clone().with_pipe_value(pipe_value.clone());
    match op.op.as_str() {
        "coalesce" => {
            if let V2EvalValue::Value(value) = &pipe_value
                && !value.is_null()
            {
                return Ok(pipe_value);
            }
            for (arg_index, arg) in op.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", step_path, arg_index);
                let value = eval_v2_expr_traced(
                    arg, record, context, out, &arg_path, &step_ctx, collector,
                )?;
                emit_v2_arg_eval(collector, &arg_path, arg_index, &op.op, &value);
                if let V2EvalValue::Value(json) = &value
                    && !json.is_null()
                {
                    return Ok(value);
                }
            }
            Ok(V2EvalValue::Missing)
        }
        "and" | "or" => {
            let is_and = op.op == "and";
            let total_len = op.args.len() + 1;
            if total_len < 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr.args must contain at least two items",
                )
                .with_path(format!("{}.args", step_path)));
            }

            let mut saw_missing = false;
            match &pipe_value {
                V2EvalValue::Missing => saw_missing = true,
                V2EvalValue::Value(value) => {
                    let flag = value_as_bool(value, step_path)?;
                    if is_and {
                        if !flag {
                            return Ok(V2EvalValue::Value(JsonValue::Bool(false)));
                        }
                    } else if flag {
                        return Ok(V2EvalValue::Value(JsonValue::Bool(true)));
                    }
                }
            }

            for (arg_index, arg) in op.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", step_path, arg_index);
                let value = eval_v2_expr_traced(
                    arg, record, context, out, &arg_path, &step_ctx, collector,
                )?;
                emit_v2_arg_eval(collector, &arg_path, arg_index, &op.op, &value);
                match value {
                    V2EvalValue::Missing => {
                        saw_missing = true;
                    }
                    V2EvalValue::Value(value) => {
                        let flag = value_as_bool(&value, &arg_path)?;
                        if is_and {
                            if !flag {
                                return Ok(V2EvalValue::Value(JsonValue::Bool(false)));
                            }
                        } else if flag {
                            return Ok(V2EvalValue::Value(JsonValue::Bool(true)));
                        }
                    }
                }
            }

            if saw_missing {
                Ok(V2EvalValue::Missing)
            } else {
                Ok(V2EvalValue::Value(JsonValue::Bool(is_and)))
            }
        }
        _ => eval_v2_op_step(op, pipe_value, record, context, out, step_path, ctx),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(in crate::transform) fn eval_v2_eager_op_traced<'a>(
    op: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    step_path: &str,
    ctx: &V2EvalContext<'a>,
    operator_metadata: Option<&'static V2OperatorMetadata>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    if matches!(pipe_value, V2EvalValue::Missing)
        && operator_metadata.is_some_and(|metadata| metadata.skips_args_when_pipe_is_missing)
    {
        return eval_v2_op_step(op, pipe_value, record, context, out, step_path, ctx);
    }

    let step_ctx = ctx.clone().with_pipe_value(pipe_value.clone());
    let mut arg_values = Vec::with_capacity(op.args.len());
    let stops_after_missing_arg =
        operator_metadata.is_some_and(|metadata| metadata.stops_after_missing_arg);
    for (arg_index, arg) in op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", step_path, arg_index);
        let value =
            eval_v2_expr_traced(arg, record, context, out, &arg_path, &step_ctx, collector)?;
        emit_v2_arg_eval(collector, &arg_path, arg_index, &op.op, &value);
        let is_missing = matches!(value, V2EvalValue::Missing);
        arg_values.push(value);
        if is_missing && stops_after_missing_arg {
            break;
        }
    }
    let cached_ctx = ctx
        .clone()
        .with_pipe_value(pipe_value.clone())
        .with_precomputed_op_args(step_path, arg_values);
    eval_v2_op_step(op, pipe_value, record, context, out, step_path, &cached_ctx)
}
