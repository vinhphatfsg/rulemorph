use serde_json::Value as JsonValue;

use super::condition::compare_values_eq;
use super::{EvalValue, V2EvalContext, eval_v2_expr, eval_value_as_string};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::V2OpStep;

mod args;

use args::resolve_lookup_args;

pub(super) fn eval_lookup_op<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    match op_step.op.as_str() {
        "lookup_first" => eval_lookup_first(op_step, pipe_value, record, context, out, path, ctx),
        "lookup" => eval_lookup(op_step, pipe_value, record, context, out, path, ctx),
        _ => unreachable!("lookup dispatcher only calls lookup operators"),
    }
}

fn eval_lookup_first<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    let Some(args) = resolve_lookup_args(op_step, pipe_value, record, context, out, path, ctx)?
    else {
        return Ok(EvalValue::Missing);
    };
    let Some(arr) = lookup_array(&args.from_value, op_step.op.as_str(), path)? else {
        return Ok(EvalValue::Missing);
    };
    let match_key = lookup_match_key(&args.match_key_value, path)?;
    if matches!(args.match_value, EvalValue::Missing) {
        return Ok(EvalValue::Missing);
    }

    for item in arr {
        if let Some(value) =
            lookup_matched_value(item, &match_key, &args.match_value, &args.get_field)
        {
            return match value {
                Some(value) => Ok(EvalValue::Value(value)),
                None => Ok(EvalValue::Missing),
            };
        }
    }

    Ok(EvalValue::Missing)
}

fn eval_lookup<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    let Some(args) = resolve_lookup_args(op_step, pipe_value, record, context, out, path, ctx)?
    else {
        return Ok(EvalValue::Missing);
    };
    let Some(arr) = lookup_array(&args.from_value, op_step.op.as_str(), path)? else {
        return Ok(EvalValue::Missing);
    };
    let match_key = lookup_match_key(&args.match_key_value, path)?;
    if matches!(args.match_value, EvalValue::Missing) {
        return Ok(EvalValue::Missing);
    }

    let mut results = Vec::new();
    for item in arr {
        if let Some(Some(value)) =
            lookup_matched_value(item, &match_key, &args.match_value, &args.get_field)
        {
            results.push(value);
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn lookup_array<'a>(
    from_value: &'a EvalValue,
    op_name: &str,
    path: &str,
) -> Result<Option<&'a Vec<JsonValue>>, TransformError> {
    match from_value {
        EvalValue::Value(JsonValue::Array(arr)) => Ok(Some(arr)),
        EvalValue::Missing => Ok(None),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("{} 'from' must be an array", op_name),
        )
        .with_path(&format!("{}.from", path))),
    }
}

fn lookup_match_key(match_key_value: &EvalValue, path: &str) -> Result<String, TransformError> {
    eval_value_as_string(match_key_value, &format!("{}.match_key", path))
}

fn lookup_matched_value(
    item: &JsonValue,
    match_key: &str,
    match_value: &EvalValue,
    get_field: &Option<String>,
) -> Option<Option<JsonValue>> {
    let JsonValue::Object(obj) = item else {
        return None;
    };
    let field_val = obj.get(match_key)?;
    let item_val = EvalValue::Value(field_val.clone());
    if !compare_values_eq(&item_val, match_value) {
        return None;
    }

    if let Some(get_key) = get_field {
        return Some(obj.get(get_key).cloned());
    }

    Some(Some(item.clone()))
}
