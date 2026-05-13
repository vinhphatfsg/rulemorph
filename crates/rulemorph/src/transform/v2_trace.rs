use super::*;
use crate::v2_operator::{V2OperatorMetadata, V2OperatorTrace, operator};

pub(super) fn eval_v2_pipe_traced<'a>(
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

fn emit_v2_arg_eval(
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

#[allow(clippy::too_many_arguments)]
fn eval_v2_lazy_op_traced<'a>(
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

#[allow(clippy::too_many_arguments)]
fn eval_v2_eager_op_traced<'a>(
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

#[allow(clippy::too_many_arguments)]
fn eval_v2_expr_traced<'a>(
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

fn v2_eval_array_from_value(
    value: V2EvalValue,
    path: &str,
) -> Result<Vec<JsonValue>, TransformError> {
    match value {
        V2EvalValue::Missing => Ok(Vec::new()),
        V2EvalValue::Value(value) => {
            if value.is_null() {
                Ok(Vec::new())
            } else if let JsonValue::Array(items) = value {
                Ok(items)
            } else {
                Err(
                    TransformError::new(TransformErrorKind::ExprError, "expr arg must be an array")
                        .with_path(path),
                )
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_expr_or_null_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<JsonValue, TransformError> {
    match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => Ok(JsonValue::Null),
        V2EvalValue::Value(value) => Ok(value),
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_predicate_expr_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => Ok(false),
        V2EvalValue::Value(value) => {
            if value.is_null() {
                Ok(false)
            } else {
                value_as_bool(&value, path)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_key_expr_string_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<String, TransformError> {
    let value = match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(path));
        }
        V2EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path));
    }
    value_to_string(&value, path)
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_sort_key_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<SortKey, TransformError> {
    let value = match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(path));
        }
        V2EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path));
    }
    sort_key_from_value(&value, path)
}

fn emit_v2_collection_item_start(
    collector: &mut TraceCollector,
    item_path: &str,
    operator: &str,
    index: usize,
    item: &JsonValue,
) {
    collector
        .start_span(TraceEventKind::CollectionItemStart, TracePhase::Start)
        .rule_path(item_path)
        .operator(operator)
        .input_path(canonical_item_path(""))
        .attr_index("item_index", index)
        .attr_enum("scope", "item")
        .input_value(item, collector.options(), Some("@item"))
        .finish(collector);
}

fn finish_v2_collection_item(
    collector: &mut TraceCollector,
    item_path: &str,
    operator: &str,
    index: usize,
    output: &V2EvalValue,
    bool_attr: Option<(&'static str, bool)>,
) {
    let mut event = collector
        .end_span(TraceEventKind::CollectionItemEnd, TracePhase::End)
        .rule_path(item_path)
        .operator(operator)
        .attr_index("item_index", index);
    if let Some((key, value)) = bool_attr {
        event = event.attr_bool(key, value);
    }
    event.finish_with_v2_eval_output(collector, output, Some("@item"));
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_collection_op_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let step_ctx = ctx.clone().with_pipe_value(pipe_value.clone());
    let operator = op_step.op.as_str();

    match operator {
        "map" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "map requires exactly one argument",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let value = eval_v2_expr_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &value);
                finish_v2_collection_item(collector, &item_path, operator, index, &value, None);
                if let V2EvalValue::Value(value) = value {
                    results.push(value);
                }
            }
            Ok(V2EvalValue::Value(JsonValue::Array(results)))
        }
        "filter" | "partition" | "find" | "find_index" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("{operator} requires exactly one argument"),
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut kept = Vec::new();
            let mut rejected = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let matches = eval_v2_predicate_expr_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                let match_value = V2EvalValue::Value(JsonValue::Bool(matches));
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &match_value);
                finish_v2_collection_item(
                    collector,
                    &item_path,
                    operator,
                    index,
                    &match_value,
                    Some(("matched", matches)),
                );
                match operator {
                    "filter" => {
                        if matches {
                            kept.push(item.clone());
                        }
                    }
                    "partition" => {
                        if matches {
                            kept.push(item.clone());
                        } else {
                            rejected.push(item.clone());
                        }
                    }
                    "find" if matches => return Ok(V2EvalValue::Value(item.clone())),
                    "find_index" if matches => {
                        return Ok(V2EvalValue::Value(JsonValue::Number((index as i64).into())));
                    }
                    _ => {}
                }
            }
            match operator {
                "filter" => Ok(V2EvalValue::Value(JsonValue::Array(kept))),
                "partition" => Ok(V2EvalValue::Value(JsonValue::Array(vec![
                    JsonValue::Array(kept),
                    JsonValue::Array(rejected),
                ]))),
                "find" => Ok(V2EvalValue::Value(JsonValue::Null)),
                "find_index" => Ok(V2EvalValue::Value(JsonValue::Number((-1).into()))),
                _ => unreachable!(),
            }
        }
        "flat_map" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "flat_map requires exactly one argument",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let value = eval_v2_expr_or_null_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                let output = V2EvalValue::Value(value.clone());
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &output);
                finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
                match value {
                    JsonValue::Array(items) => results.extend(items),
                    value => results.push(value),
                }
            }
            Ok(V2EvalValue::Value(JsonValue::Array(results)))
        }
        "group_by" | "key_by" | "distinct_by" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("{operator} requires exactly one argument"),
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut grouped = serde_json::Map::new();
            let mut keyed = serde_json::Map::new();
            let mut distinct = Vec::new();
            let mut seen = HashSet::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let key = eval_v2_key_expr_string_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                let key_output = V2EvalValue::Value(JsonValue::String(key.clone()));
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &key_output);
                let selected = match operator {
                    "group_by" => {
                        let entry = grouped
                            .entry(key)
                            .or_insert_with(|| JsonValue::Array(Vec::new()));
                        if let JsonValue::Array(items) = entry {
                            items.push(item.clone());
                        }
                        true
                    }
                    "key_by" => {
                        keyed.insert(key, item.clone());
                        true
                    }
                    "distinct_by" => {
                        if seen.insert(key) {
                            distinct.push(item.clone());
                            true
                        } else {
                            false
                        }
                    }
                    _ => unreachable!(),
                };
                finish_v2_collection_item(
                    collector,
                    &item_path,
                    operator,
                    index,
                    &key_output,
                    Some(("selected", selected)),
                );
            }
            match operator {
                "group_by" => Ok(V2EvalValue::Value(JsonValue::Object(grouped))),
                "key_by" => Ok(V2EvalValue::Value(JsonValue::Object(keyed))),
                "distinct_by" => Ok(V2EvalValue::Value(JsonValue::Array(distinct))),
                _ => unreachable!(),
            }
        }
        "sort_by" => {
            if !(1..=2).contains(&op_step.args.len()) {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "sort_by requires one or two arguments",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            if array.is_empty() {
                return Ok(V2EvalValue::Value(JsonValue::Array(Vec::new())));
            }
            let expr_path = format!("{}.args[0]", path);
            let order = if op_step.args.len() == 2 {
                let order_path = format!("{}.args[1]", path);
                let order_value = eval_v2_expr_traced(
                    &op_step.args[1],
                    record,
                    context,
                    out,
                    &order_path,
                    &step_ctx,
                    collector,
                )?;
                emit_v2_arg_eval(collector, &order_path, 1, operator, &order_value);
                let order = match order_value {
                    V2EvalValue::Missing => return Ok(V2EvalValue::Missing),
                    V2EvalValue::Value(value) => value_to_string(&value, &order_path)?,
                };
                if order != "asc" && order != "desc" {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "order must be asc or desc",
                    )
                    .with_path(order_path));
                }
                order
            } else {
                "asc".to_string()
            };

            struct TracedSortItem {
                key: SortKey,
                index: usize,
                value: JsonValue,
            }

            let mut items = Vec::with_capacity(array.len());
            let mut key_kind = None;
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let key = eval_v2_sort_key_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                    collector,
                )?;
                let kind = key.kind();
                if let Some(existing) = key_kind {
                    if existing != kind {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "sort_by keys must be all the same type",
                        )
                        .with_path(&expr_path));
                    }
                } else {
                    key_kind = Some(kind);
                }
                let key_value = sort_key_to_json(&key);
                let key_output = V2EvalValue::Value(key_value);
                emit_v2_arg_eval(collector, &expr_path, 0, operator, &key_output);
                finish_v2_collection_item(
                    collector,
                    &item_path,
                    operator,
                    index,
                    &key_output,
                    None,
                );
                items.push(TracedSortItem {
                    key,
                    index,
                    value: item.clone(),
                });
            }

            items.sort_by(|left, right| {
                let mut ordering = compare_sort_keys(&left.key, &right.key);
                if order == "desc" {
                    ordering = ordering.reverse();
                }
                if ordering == Ordering::Equal {
                    left.index.cmp(&right.index)
                } else {
                    ordering
                }
            });
            Ok(V2EvalValue::Value(JsonValue::Array(
                items.into_iter().map(|item| item.value).collect(),
            )))
        }
        "reduce" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "reduce requires exactly one argument",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            if array.is_empty() {
                return Ok(V2EvalValue::Value(JsonValue::Null));
            }
            let expr_path = format!("{}.args[0]", path);
            let mut acc = array[0].clone();
            for (index, item) in array.iter().enumerate().skip(1) {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index })
                    .with_acc(&acc);
                let value = eval_v2_expr_or_null_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                    collector,
                )?;
                let output = V2EvalValue::Value(value.clone());
                emit_v2_arg_eval(collector, &expr_path, 0, operator, &output);
                acc = value;
                finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
            }
            Ok(V2EvalValue::Value(acc))
        }
        "fold" => {
            if op_step.args.len() != 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "fold requires exactly two arguments",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let init_path = format!("{}.args[0]", path);
            let initial = eval_v2_expr_traced(
                &op_step.args[0],
                record,
                context,
                out,
                &init_path,
                &step_ctx,
                collector,
            )?;
            emit_v2_arg_eval(collector, &init_path, 0, operator, &initial);
            let mut acc = match initial {
                V2EvalValue::Missing => return Ok(V2EvalValue::Missing),
                V2EvalValue::Value(value) => value,
            };
            let expr_path = format!("{}.args[1]", path);
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index })
                    .with_acc(&acc);
                let value = eval_v2_expr_or_null_traced(
                    &op_step.args[1],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                    collector,
                )?;
                let output = V2EvalValue::Value(value.clone());
                emit_v2_arg_eval(collector, &expr_path, 1, operator, &output);
                acc = value;
                finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
            }
            Ok(V2EvalValue::Value(acc))
        }
        _ => eval_v2_op_step(op_step, pipe_value, record, context, out, path, ctx),
    }
}

pub(super) fn sort_key_to_json(key: &SortKey) -> JsonValue {
    match key {
        SortKey::Number(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        SortKey::String(value) => JsonValue::String(value.clone()),
        SortKey::Bool(value) => JsonValue::Bool(*value),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_v2_condition_traced<'a>(
    condition: &V2Condition,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    match condition {
        V2Condition::All(conditions) => {
            for (index, cond) in conditions.iter().enumerate() {
                let cond_path = format!("{}[{}]", path, index);
                if !eval_v2_condition_traced(
                    cond, record, context, out, &cond_path, ctx, collector,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        V2Condition::Any(conditions) => {
            for (index, cond) in conditions.iter().enumerate() {
                let cond_path = format!("{}[{}]", path, index);
                if eval_v2_condition_traced(cond, record, context, out, &cond_path, ctx, collector)?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        V2Condition::Comparison(comparison) => {
            eval_v2_comparison_traced(comparison, record, context, out, path, ctx, collector)
        }
        V2Condition::Expr(expr) => {
            let expr_path = format!("{}.expr", path);
            let value =
                eval_v2_expr_traced(expr, record, context, out, &expr_path, ctx, collector)?;
            match value {
                V2EvalValue::Value(JsonValue::Bool(flag)) => Ok(flag),
                V2EvalValue::Missing => Ok(false),
                V2EvalValue::Value(_) => Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "when/record_when must evaluate to boolean",
                )
                .with_path(&expr_path)),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_comparison_traced<'a>(
    comparison: &crate::v2_model::V2Comparison,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    if comparison.args.len() != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!(
                "comparison requires exactly 2 arguments, got {}",
                comparison.args.len()
            ),
        )
        .with_path(path));
    }

    let operator = v2_comparison_operator_name(comparison.op);
    collector
        .start_span(TraceEventKind::OpStart, TracePhase::Start)
        .rule_path(path)
        .operator(operator)
        .attr_count("arg_count", 2)
        .finish(collector);

    let result =
        (|| {
            let left_path = format!("{}.args[0]", path);
            let right_path = format!("{}.args[1]", path);
            let left = eval_v2_expr_traced(
                &comparison.args[0],
                record,
                context,
                out,
                &left_path,
                ctx,
                collector,
            )?;
            emit_v2_arg_eval(collector, &left_path, 0, operator, &left);
            let right = eval_v2_expr_traced(
                &comparison.args[1],
                record,
                context,
                out,
                &right_path,
                ctx,
                collector,
            )?;
            emit_v2_arg_eval(collector, &right_path, 1, operator, &right);

            match comparison.op {
                V2ComparisonOp::Eq => Ok(compare_v2_eval_eq(&left, &right)),
                V2ComparisonOp::Ne => Ok(!compare_v2_eval_eq(&left, &right)),
                V2ComparisonOp::Gt => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering == Ordering::Greater),
                V2ComparisonOp::Gte => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering != Ordering::Less),
                V2ComparisonOp::Lt => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering == Ordering::Less),
                V2ComparisonOp::Lte => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering != Ordering::Greater),
                V2ComparisonOp::Match => compare_v2_eval_match(&left, &right, path),
            }
        })();

    match result {
        Ok(flag) => {
            collector
                .end_span(TraceEventKind::OpEnd, TracePhase::End)
                .rule_path(path)
                .operator(operator)
                .finish_with_output(collector, &JsonValue::Bool(flag), None);
            Ok(flag)
        }
        Err(error) => {
            collector
                .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                .rule_path(path)
                .operator(operator)
                .finish(collector);
            Err(error)
        }
    }
}

fn v2_comparison_operator_name(op: V2ComparisonOp) -> &'static str {
    match op {
        V2ComparisonOp::Eq => "eq",
        V2ComparisonOp::Ne => "ne",
        V2ComparisonOp::Gt => "gt",
        V2ComparisonOp::Gte => "gte",
        V2ComparisonOp::Lt => "lt",
        V2ComparisonOp::Lte => "lte",
        V2ComparisonOp::Match => "match",
    }
}

fn compare_v2_eval_eq(left: &V2EvalValue, right: &V2EvalValue) -> bool {
    match (left, right) {
        (V2EvalValue::Value(left), V2EvalValue::Value(right)) => left == right,
        (V2EvalValue::Missing, V2EvalValue::Missing) => true,
        (V2EvalValue::Missing, V2EvalValue::Value(right)) => right.is_null(),
        (V2EvalValue::Value(left), V2EvalValue::Missing) => left.is_null(),
    }
}

fn compare_v2_eval_ord(
    left: &V2EvalValue,
    right: &V2EvalValue,
    path: &str,
) -> Result<Ordering, TransformError> {
    match (left, right) {
        (V2EvalValue::Value(left), V2EvalValue::Value(right)) => {
            if let (Some(left), Some(right)) = (json_value_as_f64(left), json_value_as_f64(right)) {
                return Ok(left.partial_cmp(&right).unwrap_or(Ordering::Equal));
            }
            if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
                return Ok(left.cmp(right));
            }
            Err(TransformError::new(
                TransformErrorKind::ExprError,
                "cannot compare values of different types",
            )
            .with_path(path))
        }
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "cannot compare missing values",
        )
        .with_path(path)),
    }
}

fn compare_v2_eval_match(
    left: &V2EvalValue,
    right: &V2EvalValue,
    path: &str,
) -> Result<bool, TransformError> {
    let text = match left {
        V2EvalValue::Value(JsonValue::String(value)) => value,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "match operator requires string on left side",
            )
            .with_path(path));
        }
    };
    let pattern = match right {
        V2EvalValue::Value(JsonValue::String(value)) => value,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "match operator requires regex pattern string on right side",
            )
            .with_path(path));
        }
    };
    Regex::new(pattern)
        .map_err(|err| {
            TransformError::new(
                TransformErrorKind::ExprError,
                format!("invalid regex pattern: {}", err),
            )
            .with_path(path)
        })
        .map(|regex| regex.is_match(text))
}

fn json_value_as_f64(value: &JsonValue) -> Option<f64> {
    match value {
        JsonValue::Number(number) => number.as_f64(),
        JsonValue::String(value) => value.parse::<f64>().ok(),
        _ => None,
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
