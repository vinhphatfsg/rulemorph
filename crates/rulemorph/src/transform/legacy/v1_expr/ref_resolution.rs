use super::*;

pub(in crate::transform) fn resolve_source(
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

pub(in crate::transform) fn canonical_ref_path(ref_path: &str) -> String {
    match parse_ref(ref_path) {
        Ok((Namespace::Input, path)) => canonical_input_path(path),
        Ok((Namespace::Context, path)) => canonical_context_path(path),
        Ok((Namespace::Out, path)) => canonical_out_path(path),
        Ok((Namespace::Item, path)) => canonical_item_path(path),
        Ok((Namespace::Acc, path)) => canonical_acc_path(path),
        _ => canonical_input_path(ref_path),
    }
}

pub(in crate::transform) fn eval_ref(
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
