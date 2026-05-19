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
        "lookup_first" => eval_lookup_first(op_step, pipe_value, record, context, out, path, ctx),
        "lookup" => eval_lookup(op_step, pipe_value, record, context, out, path, ctx),
        _ => unreachable!("lookup dispatcher only calls lookup operators"),
    }
}

struct LookupArgs {
    from_value: EvalValue,
    match_key_value: EvalValue,
    match_value: EvalValue,
    get_field: Option<String>,
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

fn resolve_lookup_args<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<Option<LookupArgs>, TransformError> {
    if op_step.args.len() < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!(
                "{} requires at least 2 arguments: match_key, match_value",
                op_step.op
            ),
        )
        .with_path(path));
    }

    let args = &op_step.args;
    let get_path = format!("{}.get", path);
    let resolved = match args.len() {
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
            LookupArgs {
                from_value: pipe_value,
                match_key_value,
                match_value,
                get_field: None,
            }
        }
        3 => {
            let first_value = eval_v2_expr(
                &args[0],
                record,
                context,
                out,
                &format!("{}.args[0]", path),
                ctx,
            )?;
            if matches!(pipe_value, EvalValue::Missing)
                && !matches!(first_value, EvalValue::Value(JsonValue::Array(_)))
            {
                return Ok(None);
            }

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
                LookupArgs {
                    from_value: first_value,
                    match_key_value,
                    match_value,
                    get_field: None,
                }
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
                LookupArgs {
                    from_value: pipe_value,
                    match_key_value: first_value,
                    match_value,
                    get_field: Some(eval_value_as_string(&get_value, &get_path)?),
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
            LookupArgs {
                from_value,
                match_key_value,
                match_value,
                get_field: Some(eval_value_as_string(&get_value, &get_path)?),
            }
        }
    };

    Ok(Some(resolved))
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
