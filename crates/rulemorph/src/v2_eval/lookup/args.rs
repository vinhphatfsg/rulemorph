use super::*;

pub(super) struct LookupArgs {
    pub(super) from_value: EvalValue,
    pub(super) match_key_value: EvalValue,
    pub(super) match_value: EvalValue,
    pub(super) get_field: Option<String>,
}

pub(super) fn resolve_lookup_args<'a>(
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
