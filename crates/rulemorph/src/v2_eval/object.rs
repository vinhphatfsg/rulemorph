use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::error::TransformError;
use crate::transform::GeneratedObjectBudget;
use crate::v2_model::{V2ObjectFieldValue, V2ObjectStep, object_field_rule_path};

use super::{EvalValue, V2EvalContext, eval_v2_expr};

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_v2_object_step<'a>(
    object: &V2ObjectStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    let limits = ctx.limits();
    limits.check_object_field_count(object.fields.len(), path)?;
    let mut budget = GeneratedObjectBudget::new(limits, path)?;
    let field_ctx = ctx.clone().with_pipe_value(pipe_value);
    let mut output = JsonMap::new();

    for field in &object.fields {
        let field_path = object_field_rule_path(path, &field.key);
        limits.check_object_key(&field.key, &field_path)?;
        let value = match &field.value {
            V2ObjectFieldValue::Expr(expr) => {
                eval_v2_expr(expr, record, context, out, &field_path, &field_ctx)?
            }
            V2ObjectFieldValue::Value(value) => EvalValue::Value(value.clone()),
        };
        if let EvalValue::Value(value) = value {
            budget.try_push_field(&field.key, &value, limits, &field_path)?;
            output.insert(field.key.clone(), value);
        }
    }

    let value = JsonValue::Object(output);
    limits.check_generated_json_value(&value, path)?;
    Ok(EvalValue::Value(value))
}
