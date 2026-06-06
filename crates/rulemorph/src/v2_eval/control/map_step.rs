use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::transform::{GeneratedArrayBudget, eval_custom_call_step, push_generated_array_item};
use crate::v2_eval::{
    EvalItem, EvalValue, V2EvalContext, eval_v2_if_step, eval_v2_let_step, eval_v2_object_step,
    eval_v2_op_step, eval_v2_ref,
};
use crate::v2_model::{V2MapStep, V2Step};

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
    let limits = ctx.limits();
    let mut generated_items = 0usize;
    let mut generated_json = GeneratedArrayBudget::new(limits, path)?;
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
                V2Step::Object(object_step) => {
                    current = eval_v2_object_step(
                        object_step,
                        current,
                        record,
                        context,
                        out,
                        &step_path,
                        &step_ctx,
                    )?;
                }
                V2Step::CustomCall(call_step) => {
                    current = eval_custom_call_step(
                        call_step, current, record, context, out, &step_path, &step_ctx,
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
            generated_json.try_push_value(&v, limits, path)?;
            push_generated_array_item(&mut results, v, limits, path, &mut generated_items)?;
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}
