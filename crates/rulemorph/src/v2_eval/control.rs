use serde_json::Value as JsonValue;

use super::{
    EvalItem, EvalValue, V2EvalContext, eval_v2_condition, eval_v2_op_step, eval_v2_ref,
    eval_v2_start,
};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::{V2Expr, V2IfStep, V2LetStep, V2MapStep, V2Pipe, V2Step};

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

/// Evaluate a v2 map step - iterates over arrays
pub fn eval_v2_map_step<'a>(
    map_step: &V2MapStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Get the array to iterate over
    let arr = match &pipe_value {
        EvalValue::Missing => {
            return Ok(EvalValue::Missing);
        }
        EvalValue::Value(JsonValue::Array(arr)) => arr,
        EvalValue::Value(other) => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                format!("map step requires array, got {:?}", other),
            )
            .with_path(path));
        }
    };

    // Map over each element
    let mut results = Vec::with_capacity(arr.len());
    for (index, item_value) in arr.iter().enumerate() {
        let item_path = format!("{}[{}]", path, index);

        // Create context with item scope
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item_value.clone()))
            .with_item(EvalItem {
                value: item_value,
                index,
            });

        // Apply all steps to this item
        let mut current = EvalValue::Value(item_value.clone());
        let mut step_ctx = item_ctx.clone(); // Declare outside loop to preserve let bindings

        for (step_idx, step) in map_step.steps.iter().enumerate() {
            let step_path = format!("{}.step[{}]", item_path, step_idx);
            step_ctx = step_ctx.clone().with_pipe_value(current.clone());

            match step {
                V2Step::Op(op_step) => {
                    current = eval_v2_op_step(
                        op_step, current, record, context, out, &step_path, &step_ctx,
                    )?;
                }
                V2Step::Let(let_step) => {
                    // Let in map context - evaluate and update context to preserve bindings
                    step_ctx = eval_v2_let_step(
                        let_step,
                        current.clone(),
                        record,
                        context,
                        out,
                        &step_path,
                        &step_ctx,
                    )?;
                    // Let doesn't change pipe value
                    current = step_ctx.get_pipe_value().cloned().unwrap_or(current);
                }
                V2Step::If(if_step) => {
                    current = eval_v2_if_step(
                        if_step, current, record, context, out, &step_path, &step_ctx,
                    )?;
                }
                V2Step::Map(nested_map) => {
                    current = eval_v2_map_step(
                        nested_map, current, record, context, out, &step_path, &step_ctx,
                    )?;
                }
                V2Step::Ref(v2_ref) => {
                    // Reference step evaluates the reference and returns its value
                    current = eval_v2_ref(v2_ref, record, context, out, &step_path, &step_ctx)?;
                }
            };
        }

        // Only add non-missing values to results
        if let EvalValue::Value(v) = current {
            results.push(v);
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
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
