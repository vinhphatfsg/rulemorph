use super::*;

mod keyed;
mod map;
mod predicate;
mod sort;

use keyed::eval_v2_keyed_collection_traced;
use map::{eval_v2_flat_map_traced, eval_v2_map_traced};
use predicate::eval_v2_predicate_collection_traced;
use sort::eval_v2_sort_by_traced;
pub(in crate::transform) use sort::sort_key_to_json;

pub(super) fn v2_eval_array_from_value(
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
pub(super) fn eval_v2_key_expr_string_traced<'a>(
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

pub(super) fn emit_v2_collection_item_start(
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

pub(super) fn finish_v2_collection_item(
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
pub(in crate::transform) fn eval_v2_collection_op_traced<'a>(
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
        "map" => eval_v2_map_traced(
            op_step, pipe_value, record, context, out, path, &step_ctx, collector,
        ),
        "filter" | "partition" | "find" | "find_index" => eval_v2_predicate_collection_traced(
            op_step, pipe_value, record, context, out, path, &step_ctx, collector,
        ),
        "flat_map" => eval_v2_flat_map_traced(
            op_step, pipe_value, record, context, out, path, &step_ctx, collector,
        ),
        "group_by" | "key_by" | "distinct_by" => eval_v2_keyed_collection_traced(
            op_step, pipe_value, record, context, out, path, &step_ctx, collector,
        ),
        "sort_by" => eval_v2_sort_by_traced(
            op_step, pipe_value, record, context, out, path, &step_ctx, collector,
        ),
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
