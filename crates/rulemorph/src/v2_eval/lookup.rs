use serde_json::Value as JsonValue;

use super::condition::compare_values_eq;
use super::{EvalValue, V2EvalContext, eval_v2_expr, eval_value_as_string};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::V2OpStep;

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
        // Lookup operations - v2 keyword format: lookup_first: {from: ..., match: [...], get: ...}
        // For v2, lookup args are parsed from V2OpStep with special handling
        // Explicit from:
        // args[0] = from (array to search in)
        // args[1] = match key (field name in array items to match)
        // args[2] = match value (value to match against)
        // args[3] = get (optional - field to extract from matched item)
        // Implicit from (pipe value):
        // args[0] = match key
        // args[1] = match value
        // args[2] = get (optional)
        "lookup_first" => {
            if op_step.args.len() < 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "lookup_first requires at least 2 arguments: match_key, match_value",
                )
                .with_path(path));
            }

            let args = &op_step.args;
            let from_path = format!("{}.from", path);
            let match_key_path = format!("{}.match_key", path);
            let get_path = format!("{}.get", path);

            let (from_value, match_key_value, match_value, get_field) = match args.len() {
                0 | 1 => unreachable!("guarded above"),
                2 => {
                    let match_key_value = eval_v2_expr(
                        &args[0],
                        record,
                        context,
                        out,
                        &format!("{}.args[0]", path),
                        ctx,
                    )?;
                    let match_value = eval_v2_expr(
                        &args[1],
                        record,
                        context,
                        out,
                        &format!("{}.args[1]", path),
                        ctx,
                    )?;
                    (pipe_value.clone(), match_key_value, match_value, None)
                }
                3 => {
                    if matches!(pipe_value, EvalValue::Missing) {
                        let first_value = eval_v2_expr(
                            &args[0],
                            record,
                            context,
                            out,
                            &format!("{}.args[0]", path),
                            ctx,
                        )?;
                        let use_explicit_from =
                            matches!(first_value, EvalValue::Value(JsonValue::Array(_)));
                        if !use_explicit_from {
                            return Ok(EvalValue::Missing);
                        }
                        let match_key_value = eval_v2_expr(
                            &args[1],
                            record,
                            context,
                            out,
                            &format!("{}.args[1]", path),
                            ctx,
                        )?;
                        let match_value = eval_v2_expr(
                            &args[2],
                            record,
                            context,
                            out,
                            &format!("{}.args[2]", path),
                            ctx,
                        )?;
                        (first_value, match_key_value, match_value, None)
                    } else {
                        let first_value = eval_v2_expr(
                            &args[0],
                            record,
                            context,
                            out,
                            &format!("{}.args[0]", path),
                            ctx,
                        )?;
                        let use_explicit_from = matches!(
                            first_value,
                            EvalValue::Value(JsonValue::Array(_)) | EvalValue::Missing
                        );
                        if use_explicit_from {
                            let match_key_value = eval_v2_expr(
                                &args[1],
                                record,
                                context,
                                out,
                                &format!("{}.args[1]", path),
                                ctx,
                            )?;
                            let match_value = eval_v2_expr(
                                &args[2],
                                record,
                                context,
                                out,
                                &format!("{}.args[2]", path),
                                ctx,
                            )?;
                            (first_value, match_key_value, match_value, None)
                        } else {
                            let match_value = eval_v2_expr(
                                &args[1],
                                record,
                                context,
                                out,
                                &format!("{}.args[1]", path),
                                ctx,
                            )?;
                            let get_value = eval_v2_expr(
                                &args[2],
                                record,
                                context,
                                out,
                                &format!("{}.args[2]", path),
                                ctx,
                            )?;
                            let get_field = Some(eval_value_as_string(&get_value, &get_path)?);
                            (pipe_value.clone(), first_value, match_value, get_field)
                        }
                    }
                }
                _ => {
                    let from_value = eval_v2_expr(
                        &args[0],
                        record,
                        context,
                        out,
                        &format!("{}.args[0]", path),
                        ctx,
                    )?;
                    let match_key_value = eval_v2_expr(
                        &args[1],
                        record,
                        context,
                        out,
                        &format!("{}.args[1]", path),
                        ctx,
                    )?;
                    let match_value = eval_v2_expr(
                        &args[2],
                        record,
                        context,
                        out,
                        &format!("{}.args[2]", path),
                        ctx,
                    )?;
                    let get_value = eval_v2_expr(
                        &args[3],
                        record,
                        context,
                        out,
                        &format!("{}.args[3]", path),
                        ctx,
                    )?;
                    let get_field = Some(eval_value_as_string(&get_value, &get_path)?);
                    (from_value, match_key_value, match_value, get_field)
                }
            };

            // Evaluate 'from' - the array to search in
            let arr = match &from_value {
                EvalValue::Value(JsonValue::Array(arr)) => arr,
                EvalValue::Missing => return Ok(EvalValue::Missing),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "lookup_first 'from' must be an array",
                    )
                    .with_path(&from_path));
                }
            };

            // Get match key as string
            let match_key = eval_value_as_string(&match_key_value, &match_key_path)?;
            if matches!(match_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }

            // Search for first matching item
            for item in arr {
                if let JsonValue::Object(obj) = item {
                    if let Some(field_val) = obj.get(&match_key) {
                        let item_val = EvalValue::Value(field_val.clone());
                        if compare_values_eq(&item_val, &match_value) {
                            // Found a match
                            if let Some(ref get_key) = get_field {
                                // Return specific field from matched item
                                return match obj.get(get_key) {
                                    Some(v) => Ok(EvalValue::Value(v.clone())),
                                    None => Ok(EvalValue::Missing),
                                };
                            } else {
                                // Return entire matched item
                                return Ok(EvalValue::Value(item.clone()));
                            }
                        }
                    }
                }
            }

            Ok(EvalValue::Missing)
        }

        "lookup" => {
            if op_step.args.len() < 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "lookup requires at least 2 arguments: match_key, match_value",
                )
                .with_path(path));
            }

            let args = &op_step.args;
            let from_path = format!("{}.from", path);
            let match_key_path = format!("{}.match_key", path);
            let get_path = format!("{}.get", path);

            let (from_value, match_key_value, match_value, get_field) = match args.len() {
                0 | 1 => unreachable!("guarded above"),
                2 => {
                    let match_key_value = eval_v2_expr(
                        &args[0],
                        record,
                        context,
                        out,
                        &format!("{}.args[0]", path),
                        ctx,
                    )?;
                    let match_value = eval_v2_expr(
                        &args[1],
                        record,
                        context,
                        out,
                        &format!("{}.args[1]", path),
                        ctx,
                    )?;
                    (pipe_value.clone(), match_key_value, match_value, None)
                }
                3 => {
                    if matches!(pipe_value, EvalValue::Missing) {
                        let first_value = eval_v2_expr(
                            &args[0],
                            record,
                            context,
                            out,
                            &format!("{}.args[0]", path),
                            ctx,
                        )?;
                        let use_explicit_from =
                            matches!(first_value, EvalValue::Value(JsonValue::Array(_)));
                        if !use_explicit_from {
                            return Ok(EvalValue::Missing);
                        }
                        let match_key_value = eval_v2_expr(
                            &args[1],
                            record,
                            context,
                            out,
                            &format!("{}.args[1]", path),
                            ctx,
                        )?;
                        let match_value = eval_v2_expr(
                            &args[2],
                            record,
                            context,
                            out,
                            &format!("{}.args[2]", path),
                            ctx,
                        )?;
                        (first_value, match_key_value, match_value, None)
                    } else {
                        let first_value = eval_v2_expr(
                            &args[0],
                            record,
                            context,
                            out,
                            &format!("{}.args[0]", path),
                            ctx,
                        )?;
                        let use_explicit_from = matches!(
                            first_value,
                            EvalValue::Value(JsonValue::Array(_)) | EvalValue::Missing
                        );
                        if use_explicit_from {
                            let match_key_value = eval_v2_expr(
                                &args[1],
                                record,
                                context,
                                out,
                                &format!("{}.args[1]", path),
                                ctx,
                            )?;
                            let match_value = eval_v2_expr(
                                &args[2],
                                record,
                                context,
                                out,
                                &format!("{}.args[2]", path),
                                ctx,
                            )?;
                            (first_value, match_key_value, match_value, None)
                        } else {
                            let match_value = eval_v2_expr(
                                &args[1],
                                record,
                                context,
                                out,
                                &format!("{}.args[1]", path),
                                ctx,
                            )?;
                            let get_value = eval_v2_expr(
                                &args[2],
                                record,
                                context,
                                out,
                                &format!("{}.args[2]", path),
                                ctx,
                            )?;
                            let get_field = Some(eval_value_as_string(&get_value, &get_path)?);
                            (pipe_value.clone(), first_value, match_value, get_field)
                        }
                    }
                }
                _ => {
                    let from_value = eval_v2_expr(
                        &args[0],
                        record,
                        context,
                        out,
                        &format!("{}.args[0]", path),
                        ctx,
                    )?;
                    let match_key_value = eval_v2_expr(
                        &args[1],
                        record,
                        context,
                        out,
                        &format!("{}.args[1]", path),
                        ctx,
                    )?;
                    let match_value = eval_v2_expr(
                        &args[2],
                        record,
                        context,
                        out,
                        &format!("{}.args[2]", path),
                        ctx,
                    )?;
                    let get_value = eval_v2_expr(
                        &args[3],
                        record,
                        context,
                        out,
                        &format!("{}.args[3]", path),
                        ctx,
                    )?;
                    let get_field = Some(eval_value_as_string(&get_value, &get_path)?);
                    (from_value, match_key_value, match_value, get_field)
                }
            };

            // Evaluate 'from' - the array to search in
            let arr = match &from_value {
                EvalValue::Value(JsonValue::Array(arr)) => arr,
                EvalValue::Missing => return Ok(EvalValue::Missing),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "lookup 'from' must be an array",
                    )
                    .with_path(&from_path));
                }
            };

            // Get match key as string
            let match_key = eval_value_as_string(&match_key_value, &match_key_path)?;
            if matches!(match_value, EvalValue::Missing) {
                return Ok(EvalValue::Missing);
            }

            // Search for ALL matching items
            let mut results = Vec::new();
            for item in arr {
                if let JsonValue::Object(obj) = item {
                    if let Some(field_val) = obj.get(&match_key) {
                        let item_val = EvalValue::Value(field_val.clone());
                        if compare_values_eq(&item_val, &match_value) {
                            // Found a match
                            if let Some(ref get_key) = get_field {
                                // Add specific field from matched item
                                if let Some(v) = obj.get(get_key) {
                                    results.push(v.clone());
                                }
                            } else {
                                // Add entire matched item
                                results.push(item.clone());
                            }
                        }
                    }
                }
            }

            Ok(EvalValue::Value(JsonValue::Array(results)))
        }
        _ => unreachable!("lookup dispatcher only calls lookup operators"),
    }
}
