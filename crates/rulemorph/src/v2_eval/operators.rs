use serde_json::Value as JsonValue;

mod number;
mod projection;
mod special;
mod string;
mod typed_value;

use self::number::eval_number_op;
use self::projection::eval_projection_op;
use self::special::{eval_coalesce_op, eval_first_last_op, eval_logical_op, eval_not_op};
use self::string::eval_string_op;
use self::typed_value::eval_typed_value_op;
use super::{
    EvalValue, V2EvalContext, eval_collection_op, eval_comparison_op, eval_lookup_op,
    eval_type_cast, eval_v2_op_with_v1_fallback, eval_v2_ref,
};
use crate::error::{TransformError, TransformErrorKind};
use crate::transform::eval_custom_op_step;
use crate::v2_model::V2OpStep;
use crate::v2_operator::{operator, operator_arg_range};

/// Evaluate a v2 op step with a pipe value as implicit first argument
pub fn eval_v2_op_step<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Create a new context with the current pipe value
    let step_ctx = ctx.clone().with_pipe_value(pipe_value.clone());

    // Handle "@..." as a reference (from shorthand string in step position)
    if op_step.op.starts_with('@') {
        use crate::v2_parser::parse_v2_ref;
        if let Some(v2_ref) = parse_v2_ref(&op_step.op) {
            return eval_v2_ref(&v2_ref, record, context, out, path, &step_ctx);
        }
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid reference: {}", op_step.op),
        )
        .with_path(path));
    }

    if let Some(value) = eval_custom_op_step(
        op_step,
        pipe_value.clone(),
        record,
        context,
        out,
        path,
        &step_ctx,
    )? {
        return Ok(value);
    }

    validate_v2_op_runtime_args(op_step, path)?;

    match op_step.op.as_str() {
        // String operations
        "trim" | "lowercase" | "uppercase" | "to_string" | "concat" => {
            eval_string_op(op_step, pipe_value, record, context, out, path, &step_ctx)
        }
        "string" | "int" | "float" | "bool" => {
            eval_type_cast(op_step.op.as_str(), &pipe_value, path)
        }

        // Numeric operations
        "add" | "+" | "subtract" | "-" | "multiply" | "*" | "divide" | "/" => {
            eval_number_op(op_step, pipe_value, record, context, out, path, &step_ctx)
        }
        "map" | "filter" | "flat_map" | "group_by" | "key_by" | "partition" | "distinct_by"
        | "sort_by" | "find" | "find_index" | "reduce" | "fold" | "zip_with" => {
            eval_collection_op(op_step, pipe_value, record, context, out, path, &step_ctx)
        }
        "first" | "last" => eval_first_last_op(op_step, pipe_value, path),

        // Coalesce
        "coalesce" => eval_coalesce_op(op_step, pipe_value, record, context, out, path, &step_ctx),
        "and" | "or" => eval_logical_op(op_step, pipe_value, record, context, out, path, &step_ctx),
        "not" => eval_not_op(op_step, pipe_value, path),
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" | "eq" | "ne" | "lt" | "lte" | "gt"
        | "gte" | "match" => {
            eval_comparison_op(op_step, pipe_value, record, context, out, path, &step_ctx)
        }
        "pick" | "omit" => {
            eval_projection_op(op_step, pipe_value, record, context, out, path, &step_ctx)
        }

        "to_typed_value" | "from_typed_value" => {
            eval_typed_value_op(op_step, pipe_value, record, context, out, path, &step_ctx)
        }

        "lookup_first" | "lookup" => {
            eval_lookup_op(op_step, pipe_value, record, context, out, path, &step_ctx)
        }

        // Default case - fall back to v1 op evaluation
        _ => {
            eval_v2_op_with_v1_fallback(op_step, pipe_value, record, context, out, path, &step_ctx)
        }
    }
}

fn validate_v2_op_runtime_args(op_step: &V2OpStep, path: &str) -> Result<(), TransformError> {
    if op_step.op == "range" {
        return Ok(());
    }
    if operator(&op_step.op).is_none() {
        return Ok(());
    }

    let count = op_step.args.len();
    let (min, max) = operator_arg_range(&op_step.op);
    if count < min {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!(
                "{} requires at least {} argument(s), got {}",
                op_step.op, min, count
            ),
        )
        .with_path(path));
    }
    if let Some(max) = max
        && count > max
    {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!(
                "{} accepts at most {} argument(s), got {}",
                op_step.op, max, count
            ),
        )
        .with_path(path));
    }

    Ok(())
}
