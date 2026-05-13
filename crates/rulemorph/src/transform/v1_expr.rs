use super::*;

pub(super) fn eval_when(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
) -> bool {
    let expr = match &mapping.when {
        Some(expr) => expr,
        None => return true,
    };

    let when_path = format!("{}.when", mapping_path);
    match eval_when_expr(expr, record, context, out, &when_path, rule_version) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_when_traced(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    collector: &mut TraceCollector,
) -> bool {
    let expr = match &mapping.when {
        Some(expr) => expr,
        None => return true,
    };

    let when_path = format!("{}.when", mapping_path);
    match eval_when_expr_traced(
        expr,
        record,
        context,
        out,
        &when_path,
        rule_version,
        collector,
    ) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

pub(super) fn eval_record_when(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
) -> bool {
    let expr = match &rule.record_when {
        Some(expr) => expr,
        None => return true,
    };

    let empty_out = JsonValue::Object(Map::new());
    match eval_when_expr(
        expr,
        record,
        context,
        &empty_out,
        "record_when",
        rule.version,
    ) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

pub(super) fn eval_record_when_traced(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    collector: &mut TraceCollector,
) -> bool {
    let expr = match &rule.record_when {
        Some(expr) => expr,
        None => return true,
    };

    let empty_out = JsonValue::Object(Map::new());
    match eval_when_expr_traced(
        expr,
        record,
        context,
        &empty_out,
        "record_when",
        rule.version,
        collector,
    ) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

fn eval_bool_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
) -> Result<bool, TransformError> {
    let value = eval_expr(expr, record, context, out, path, None)?;
    let value = match value {
        EvalValue::Missing => JsonValue::Null,
        EvalValue::Value(value) => value,
    };
    match value {
        JsonValue::Bool(flag) => Ok(flag),
        _ => Err(when_type_error(path)),
    }
}

fn eval_bool_expr_traced(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    let value = eval_expr_traced(expr, record, context, out, path, None, collector)?;
    let value = match value {
        EvalValue::Missing => JsonValue::Null,
        EvalValue::Value(value) => value,
    };
    match value {
        JsonValue::Bool(flag) => Ok(flag),
        _ => Err(when_type_error(path)),
    }
}

pub(super) fn eval_when_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    rule_version: u8,
) -> Result<bool, TransformError> {
    if rule_version >= 2 {
        if let Some(raw_value) = expr_to_json_for_v2_condition(expr) {
            let condition = parse_v2_condition(&raw_value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 condition: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new();
            return eval_v2_condition(&condition, record, context, out, path, &ctx);
        }
    }

    eval_bool_expr(expr, record, context, out, path)
}

pub(super) fn eval_when_expr_traced(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    rule_version: u8,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    if rule_version >= 2 {
        if let Some(raw_value) = expr_to_json_for_v2_condition(expr) {
            let condition = parse_v2_condition(&raw_value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 condition: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new();
            return eval_v2_condition_traced(
                &condition, record, context, out, path, &ctx, collector,
            );
        }
    }

    eval_bool_expr_traced(expr, record, context, out, path, collector)
}

fn when_type_error(path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::ExprError,
        "when/record_when must evaluate to boolean",
    )
    .with_path(path)
}

pub(super) fn resolve_source(
    source: &str,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
) -> Result<EvalValue, TransformError> {
    let (namespace, path) =
        parse_source(source).map_err(|err| err.with_path(format!("{}.source", mapping_path)))?;
    let tokens = parse_path_tokens(
        path,
        TransformErrorKind::InvalidRef,
        format!("{}.source", mapping_path),
    )?;
    let target = match namespace {
        Namespace::Input => Some(record),
        Namespace::Context => context,
        Namespace::Out => Some(out),
        Namespace::Item | Namespace::Acc | Namespace::Pipe | Namespace::Local => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "ref namespace must be input|context|out",
            )
            .with_path(format!("{}.source", mapping_path)));
        }
    };

    match target.and_then(|value| get_path(value, &tokens)) {
        Some(value) => Ok(EvalValue::Value(value.clone())),
        None => Ok(EvalValue::Missing),
    }
}

pub(super) fn eval_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    match expr {
        Expr::Literal(value) => Ok(EvalValue::Value(value.clone())),
        Expr::Ref(expr_ref) => eval_ref(expr_ref, record, context, out, base_path, locals),
        Expr::Op(expr_op) => eval_op(expr_op, record, context, out, base_path, None, locals),
        Expr::Chain(expr_chain) => eval_chain(expr_chain, record, context, out, base_path, locals),
    }
}

pub(super) fn canonical_ref_path(ref_path: &str) -> String {
    match parse_ref(ref_path) {
        Ok((Namespace::Input, path)) => canonical_input_path(path),
        Ok((Namespace::Context, path)) => canonical_context_path(path),
        Ok((Namespace::Out, path)) => canonical_out_path(path),
        Ok((Namespace::Item, path)) => canonical_item_path(path),
        Ok((Namespace::Acc, path)) => canonical_acc_path(path),
        _ => canonical_input_path(ref_path),
    }
}

pub(super) fn eval_chain(
    expr_chain: &ExprChain,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    if expr_chain.chain.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.chain must be a non-empty array",
        )
        .with_path(format!("{}.chain", base_path)));
    }

    let first_path = format!("{}.chain[0]", base_path);
    let mut current = eval_expr(
        &expr_chain.chain[0],
        record,
        context,
        out,
        &first_path,
        locals,
    )?;

    for (index, step) in expr_chain.chain.iter().enumerate().skip(1) {
        let step_path = format!("{}.chain[{}]", base_path, index);
        let expr_op = match step {
            Expr::Op(expr_op) => expr_op,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr.chain items after first must be op",
                )
                .with_path(step_path));
            }
        };

        let injected = current.clone();
        current = eval_op(
            expr_op,
            record,
            context,
            out,
            &step_path,
            Some(&injected),
            locals,
        )?;
    }

    Ok(current)
}

pub(super) fn eval_ref(
    expr_ref: &ExprRef,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let (namespace, path) =
        parse_ref(&expr_ref.ref_path).map_err(|err| err.with_path(base_path))?;
    let tokens = parse_path_tokens(path, TransformErrorKind::InvalidRef, base_path.to_string())?;
    let target = match namespace {
        Namespace::Input => Some(record),
        Namespace::Context => context,
        Namespace::Out => Some(out),
        Namespace::Item => {
            let item = locals.and_then(|locals| locals.item).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "item is only available within array ops",
                )
                .with_path(base_path)
            })?;
            let (root, rest) = match tokens.split_first() {
                Some((PathToken::Key(key), rest)) if key == "value" => (item.value, rest),
                Some((PathToken::Key(key), rest)) if key == "index" => {
                    if !rest.is_empty() {
                        return Ok(EvalValue::Missing);
                    }
                    let value = JsonValue::Number(serde_json::Number::from(item.index as u64));
                    return Ok(EvalValue::Value(value));
                }
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "item ref must start with value or index",
                    )
                    .with_path(base_path));
                }
            };
            return match get_path(root, rest) {
                Some(value) => Ok(EvalValue::Value(value.clone())),
                None => Ok(EvalValue::Missing),
            };
        }
        Namespace::Acc => {
            let acc = locals.and_then(|locals| locals.acc).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "acc is only available within reduce/fold ops",
                )
                .with_path(base_path)
            })?;
            let (root, rest) = match tokens.split_first() {
                Some((PathToken::Key(key), rest)) if key == "value" => (acc, rest),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "acc ref must start with value",
                    )
                    .with_path(base_path));
                }
            };
            return match get_path(root, rest) {
                Some(value) => Ok(EvalValue::Value(value.clone())),
                None => Ok(EvalValue::Missing),
            };
        }
        Namespace::Pipe => {
            let pipe_value = locals.and_then(|locals| locals.pipe).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "pipe is only available within v2 pipes",
                )
                .with_path(base_path)
            })?;
            let (root, rest) = match tokens.split_first() {
                Some((PathToken::Key(key), rest)) if key == "value" => (pipe_value, rest),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "pipe ref must start with value",
                    )
                    .with_path(base_path));
                }
            };
            let value = match root {
                EvalValue::Missing => return Ok(EvalValue::Missing),
                EvalValue::Value(value) => value,
            };
            return match get_path(value, rest) {
                Some(value) => Ok(EvalValue::Value(value.clone())),
                None => Ok(EvalValue::Missing),
            };
        }
        Namespace::Local => {
            let locals_map = locals.and_then(|locals| locals.locals).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "local is only available within v2 pipes",
                )
                .with_path(base_path)
            })?;
            let (first, rest) = match tokens.split_first() {
                Some((PathToken::Key(key), rest)) => (key, rest),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "local ref must start with a key",
                    )
                    .with_path(base_path));
                }
            };
            let local_value = locals_map.get(first).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("undefined local: {}", first),
                )
                .with_path(base_path)
            })?;
            let value = match local_value {
                EvalValue::Missing => return Ok(EvalValue::Missing),
                EvalValue::Value(value) => value,
            };
            return match get_path(value, rest) {
                Some(value) => Ok(EvalValue::Value(value.clone())),
                None => Ok(EvalValue::Missing),
            };
        }
    };

    match target.and_then(|value| get_path(value, &tokens)) {
        Some(value) => Ok(EvalValue::Value(value.clone())),
        None => Ok(EvalValue::Missing),
    }
}
