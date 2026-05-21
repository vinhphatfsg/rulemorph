use serde_json::Value as JsonValue;

use super::eval_v2_expr;
use crate::error::TransformError;
use crate::v2_eval::{EvalValue, V2EvalContext};
use crate::v2_model::V2LetStep;

/// Evaluate a v2 let step - binds variables to context without changing pipe value
pub fn eval_v2_let_step<'a>(
    let_step: &V2LetStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<V2EvalContext<'a>, TransformError> {
    let mut new_ctx = ctx.clone().with_pipe_value(pipe_value);

    for (name, expr) in &let_step.bindings {
        let binding_path = format!("{}.{}", path, name);
        let value = eval_v2_expr(expr, record, context, out, &binding_path, &new_ctx)?;
        new_ctx = new_ctx.with_let_binding(name.clone(), value);
    }

    Ok(new_ctx)
}
