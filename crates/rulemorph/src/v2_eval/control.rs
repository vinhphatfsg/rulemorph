use serde_json::Value as JsonValue;

mod if_step;
mod let_step;
mod map_step;

pub use if_step::eval_v2_if_step;
pub use let_step::eval_v2_let_step;
pub use map_step::eval_v2_map_step;

use super::{EvalValue, V2EvalContext, eval_v2_op_step, eval_v2_ref, eval_v2_start};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::{V2Expr, V2Pipe, V2Step};

/// Evaluate a v2 pipe expression
pub fn eval_v2_pipe<'a>(
    pipe: &V2Pipe,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Evaluate start value
    let mut current = eval_v2_start(&pipe.start, record, context, out, path, ctx)?;
    let mut current_ctx = ctx.clone();

    // Apply each step
    for (i, step) in pipe.steps.iter().enumerate() {
        let step_path = format!("{}[{}]", path, i + 1);
        // Update context with current pipe value for each step
        current_ctx = current_ctx.clone().with_pipe_value(current.clone());

        match step {
            V2Step::Op(op_step) => {
                current = eval_v2_op_step(
                    op_step,
                    current,
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                )?;
            }
            V2Step::Let(let_step) => {
                // Let step doesn't change pipe value, just adds bindings to context
                current_ctx = eval_v2_let_step(
                    let_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                )?;
            }
            V2Step::If(if_step) => {
                current = eval_v2_if_step(
                    if_step,
                    current,
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                )?;
            }
            V2Step::Map(map_step) => {
                current = eval_v2_map_step(
                    map_step,
                    current,
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                )?;
            }
            V2Step::Ref(v2_ref) => {
                // Reference step evaluates the reference and returns its value
                current = eval_v2_ref(v2_ref, record, context, out, &step_path, &current_ctx)?;
            }
        }
    }

    Ok(current)
}

/// Evaluate a v2 expression
pub fn eval_v2_expr<'a>(
    expr: &V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    if let Some(value) = ctx.precomputed_arg_for_path(path) {
        return Ok(value);
    }
    match expr {
        V2Expr::Pipe(pipe) => eval_v2_pipe(pipe, record, context, out, path, ctx),
        V2Expr::V1Fallback(_) => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "v1 fallback not yet implemented",
        )
        .with_path(path)),
    }
}
