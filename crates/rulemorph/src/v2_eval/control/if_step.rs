use serde_json::Value as JsonValue;

use super::eval_v2_pipe;
use crate::error::TransformError;
use crate::v2_eval::{EvalValue, V2EvalContext, eval_v2_condition};
use crate::v2_model::V2IfStep;

/// Evaluate a v2 if step - conditional branching
pub fn eval_v2_if_step<'a>(
    if_step: &V2IfStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Create context with current pipe value for condition evaluation
    let cond_ctx = ctx.clone().with_pipe_value(pipe_value.clone());

    // Evaluate condition
    let cond_path = format!("{}.cond", path);
    let cond_result =
        eval_v2_condition(&if_step.cond, record, context, out, &cond_path, &cond_ctx)?;

    if cond_result {
        // Execute then branch
        let then_path = format!("{}.then", path);
        eval_v2_pipe(
            &if_step.then_branch,
            record,
            context,
            out,
            &then_path,
            &cond_ctx,
        )
    } else if let Some(ref else_branch) = if_step.else_branch {
        // Execute else branch
        let else_path = format!("{}.else", path);
        eval_v2_pipe(else_branch, record, context, out, &else_path, &cond_ctx)
    } else {
        // No else branch, return pipe value unchanged
        Ok(pipe_value)
    }
}
