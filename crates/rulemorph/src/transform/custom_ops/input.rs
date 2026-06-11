use super::*;

pub(super) fn eval_with_object<'a>(
    with: &[(String, V2CallArg)],
    input_type: &RuleType,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<JsonValue, TransformError> {
    let mut fields = Vec::new();
    for (name, arg) in with {
        let value = match arg {
            V2CallArg::Value(value) => value.clone(),
            V2CallArg::Expr(expr) => match eval_v2_expr(
                expr,
                record,
                context,
                out,
                &format!("{}.with.{}", path, name),
                ctx,
            )? {
                V2EvalValue::Value(value) => value,
                V2EvalValue::Missing if is_optional_input_field(input_type, name) => continue,
                V2EvalValue::Missing => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "custom op input contract mismatch: with value is missing",
                    )
                    .with_path(format!("{}.with.{}", path, name)));
                }
            },
        };
        fields.push((name.clone(), value));
    }
    Ok(custom_ops::build_with_object(fields))
}

pub(super) struct TracedCustomInput {
    pub(super) value: JsonValue,
    pub(super) redaction_hints: CustomInputRedactionHints,
}

#[derive(Default)]
pub(super) struct CustomInputRedactionHints {
    fields: BTreeMap<String, Option<String>>,
}

pub(super) enum RedactionHintOverride {
    Hint(String),
    Unknown,
}

impl CustomInputRedactionHints {
    pub(super) fn redaction_hint_override(&self, path: &str) -> Option<RedactionHintOverride> {
        let (field, suffix) = local_input_field_ref(path)?;
        match self.fields.get(&field)? {
            Some(path_hint) => Some(RedactionHintOverride::Hint(append_path_suffix(
                path_hint, suffix,
            ))),
            None => Some(RedactionHintOverride::Unknown),
        }
    }

    pub(super) fn body_redaction_hint_override(&self, path: &str) -> Option<RedactionHintOverride> {
        let Some((field, suffix)) = local_input_field_ref(path) else {
            return valid_local_input_path(path).then_some(RedactionHintOverride::Unknown);
        };
        match self.fields.get(&field) {
            Some(Some(path_hint)) => Some(RedactionHintOverride::Hint(append_path_suffix(
                path_hint, suffix,
            ))),
            Some(None) | None => Some(RedactionHintOverride::Unknown),
        }
    }
}

pub(super) fn local_input_field_ref(path: &str) -> Option<(String, String)> {
    let path = local_input_path(path)?;

    if path.starts_with('[') {
        let tokens = parse_path(path).ok()?;
        let Some(PathToken::Key(field)) = tokens.first() else {
            return None;
        };
        return Some((field.clone(), path_token_suffix(&tokens[1..])));
    }

    let boundary = path.find(['.', '[']).unwrap_or(path.len());
    if boundary == 0 {
        return None;
    }
    Some((path[..boundary].to_string(), path[boundary..].to_string()))
}

pub(super) fn valid_local_input_path(path: &str) -> bool {
    local_input_path(path).is_some_and(|path| parse_path(path).is_ok())
}

pub(super) fn local_input_path(path: &str) -> Option<&str> {
    let path = if let Some(path) = path.strip_prefix("$.") {
        path
    } else if path.starts_with("$[") {
        &path[1..]
    } else if let Some(path) = path.strip_prefix("@input.") {
        path
    } else if path.starts_with("@input[") {
        &path["@input".len()..]
    } else if path == "$" || path == "@input" || path.starts_with('@') {
        return None;
    } else {
        path
    };
    Some(path)
}

pub(super) fn path_token_suffix(tokens: &[PathToken]) -> String {
    let mut suffix = String::new();
    for token in tokens {
        match token {
            PathToken::Key(key) => {
                suffix.push_str("[\"");
                for ch in key.chars() {
                    if ch == '\\' || ch == '"' {
                        suffix.push('\\');
                    }
                    suffix.push(ch);
                }
                suffix.push_str("\"]");
            }
            PathToken::Index(index) => {
                suffix.push('[');
                suffix.push_str(&index.to_string());
                suffix.push(']');
            }
        }
    }
    suffix
}

pub(super) fn append_path_suffix(path_hint: &str, suffix: String) -> String {
    if suffix.is_empty() {
        path_hint.to_string()
    } else {
        format!("{}{}", path_hint, suffix)
    }
}

pub(super) fn custom_input_field_redaction_hint(
    arg: &V2CallArg,
    custom_body_input_scope: bool,
) -> Option<String> {
    match arg {
        V2CallArg::Expr(expr) => v2_expr_redaction_hint(expr, custom_body_input_scope),
        V2CallArg::Value(_) => None,
    }
}

pub(super) fn v2_expr_redaction_hint(
    expr: &V2Expr,
    custom_body_input_scope: bool,
) -> Option<String> {
    let mut hint = RedactionHint {
        text: String::new(),
        unknown_provenance: false,
        custom_body_input_scope,
    };
    collect_v2_expr_redaction_hints(expr, &mut hint, &CustomInputRedactionHints::default());
    if hint.unknown_provenance {
        return None;
    }
    let text = hint.text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

pub(super) struct TracedWithObjectInput<'a, 'collector> {
    pub(super) with: &'a [(String, V2CallArg)],
    pub(super) input_type: &'a RuleType,
    pub(super) record: &'a JsonValue,
    pub(super) context: Option<&'a JsonValue>,
    pub(super) out: &'a JsonValue,
    pub(super) path: &'a str,
    pub(super) custom_body_input_scope: bool,
    pub(super) ctx: &'a V2EvalContext<'a>,
    pub(super) collector: &'collector mut TraceCollector,
}

pub(super) fn eval_with_object_traced(
    input: TracedWithObjectInput<'_, '_>,
) -> Result<TracedCustomInput, TransformError> {
    let TracedWithObjectInput {
        with,
        input_type,
        record,
        context,
        out,
        path,
        custom_body_input_scope,
        ctx,
        collector,
    } = input;

    let mut fields = Vec::new();
    let mut redaction_hints = CustomInputRedactionHints::default();
    for (name, arg) in with {
        let arg_path = format!("{}.with.{}", path, name);
        let redaction_hint = custom_input_field_redaction_hint(arg, custom_body_input_scope);
        let value = match arg {
            V2CallArg::Value(value) => {
                collector
                    .emit(TraceEventKind::LiteralEval, TracePhase::Instant)
                    .rule_path(&arg_path)
                    .finish_with_output(collector, value, None);
                value.clone()
            }
            V2CallArg::Expr(expr) => {
                match eval_v2_expr_traced(expr, record, context, out, &arg_path, ctx, collector)? {
                    V2EvalValue::Value(value) => value,
                    V2EvalValue::Missing if is_optional_input_field(input_type, name) => continue,
                    V2EvalValue::Missing => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "custom op input contract mismatch: with value is missing",
                        )
                        .with_path(arg_path));
                    }
                }
            }
        };
        redaction_hints.fields.insert(name.clone(), redaction_hint);
        fields.push((name.clone(), value));
    }
    Ok(TracedCustomInput {
        value: custom_ops::build_with_object(fields),
        redaction_hints,
    })
}

pub(super) fn is_optional_input_field(input_type: &RuleType, name: &str) -> bool {
    match &input_type.kind {
        RuleTypeKind::Object(fields) => fields.get(name).is_some_and(|field| field.optional),
        _ => false,
    }
}
