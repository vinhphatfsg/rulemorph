use super::*;

pub(super) fn mapping_output_redaction_hint(
    mapping: &Mapping,
    input_redaction_hints: &CustomInputRedactionHints,
) -> Option<String> {
    let mut hint = RedactionHint {
        text: mapping.target.clone(),
        unknown_provenance: false,
        custom_body_input_scope: true,
    };
    if mapping.value.is_some() || mapping.default.is_some() {
        hint.unknown_provenance = true;
    }
    if let Some(source) = &mapping.source {
        collect_path_redaction_hint(source, &mut hint, input_redaction_hints);
    }
    if let Some(expr) = &mapping.expr {
        collect_expr_redaction_hints(expr, &mut hint, input_redaction_hints);
    }
    if hint.unknown_provenance {
        None
    } else {
        Some(hint.text)
    }
}

pub(super) struct RedactionHint {
    pub(super) text: String,
    pub(super) unknown_provenance: bool,
    pub(super) custom_body_input_scope: bool,
}

pub(super) fn collect_expr_redaction_hints(
    expr: &Expr,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match expr {
        Expr::Ref(expr_ref) => {
            collect_path_redaction_hint(&expr_ref.ref_path, hint, input_redaction_hints);
        }
        Expr::Op(expr_op) => {
            for arg in &expr_op.args {
                collect_expr_redaction_hints(arg, hint, input_redaction_hints);
            }
        }
        Expr::Chain(expr_chain) => {
            for part in &expr_chain.chain {
                collect_expr_redaction_hints(part, hint, input_redaction_hints);
            }
        }
        Expr::Literal(value) => collect_json_redaction_hints(value, hint, input_redaction_hints),
    }
}

pub(super) fn collect_json_redaction_hints(
    _value: &JsonValue,
    hint: &mut RedactionHint,
    _input_redaction_hints: &CustomInputRedactionHints,
) {
    hint.unknown_provenance = true;
}

pub(super) fn collect_path_redaction_hint(
    path: &str,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    if path == "$" || path == "@input" || (!hint.custom_body_input_scope && path.starts_with("$."))
    {
        hint.unknown_provenance = true;
        return;
    }
    let override_hint = if hint.custom_body_input_scope {
        custom_body_path_redaction_hint_override(path, input_redaction_hints)
    } else {
        input_redaction_hints.redaction_hint_override(path)
    };
    match override_hint {
        Some(RedactionHintOverride::Hint(path_hint)) => {
            hint.text.push(' ');
            hint.text.push_str(&path_hint);
        }
        Some(RedactionHintOverride::Unknown) => {
            hint.unknown_provenance = true;
        }
        None => {}
    }
    hint.text.push(' ');
    hint.text.push_str(path);
}

pub(super) fn custom_body_path_redaction_hint_override(
    path: &str,
    input_redaction_hints: &CustomInputRedactionHints,
) -> Option<RedactionHintOverride> {
    if path == "$" || path == "@input" {
        return Some(RedactionHintOverride::Unknown);
    }
    if path.starts_with("$.")
        || path.starts_with("$[")
        || path.starts_with("@input.")
        || path.starts_with("@input[")
    {
        return input_redaction_hints.body_redaction_hint_override(path);
    }
    if is_body_local_output_ref(path) || is_body_local_unknown_ref(path) {
        return Some(RedactionHintOverride::Unknown);
    }
    match parse_source(path) {
        Ok((Namespace::Input, input_path)) => {
            input_redaction_hints.body_redaction_hint_override(input_path)
        }
        Ok((Namespace::Context, context_path)) => Some(RedactionHintOverride::Hint(
            canonical_context_path(context_path),
        )),
        Ok((Namespace::Out, _))
        | Ok((Namespace::Item | Namespace::Acc | Namespace::Pipe | Namespace::Local, _)) => {
            Some(RedactionHintOverride::Unknown)
        }
        Err(_) => None,
    }
}

pub(super) fn is_body_local_output_ref(path: &str) -> bool {
    matches!(path, "@out" | "out")
        || path.starts_with("@out.")
        || path.starts_with("@out[")
        || path.starts_with("out.")
        || path.starts_with("out[")
}

pub(super) fn is_body_local_unknown_ref(path: &str) -> bool {
    matches!(path, "@local" | "local" | "@acc" | "acc" | "@item" | "item")
        || path.starts_with("@local.")
        || path.starts_with("@local[")
        || path.starts_with("local.")
        || path.starts_with("local[")
        || path.starts_with("@acc.")
        || path.starts_with("@acc[")
        || path.starts_with("acc.")
        || path.starts_with("acc[")
        || path.starts_with("@item.")
        || path.starts_with("@item[")
        || path.starts_with("item.")
        || path.starts_with("item[")
}

pub(super) fn collect_v2_expr_redaction_hints(
    expr: &V2Expr,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match expr {
        V2Expr::Pipe(pipe) => {
            collect_v2_pipe_redaction_hints(pipe, hint, input_redaction_hints);
        }
        V2Expr::V1Fallback(expr) => {
            collect_expr_redaction_hints(expr, hint, input_redaction_hints);
        }
    }
}

pub(super) fn collect_v2_pipe_redaction_hints(
    pipe: &V2Pipe,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    collect_v2_start_redaction_hints(&pipe.start, hint, input_redaction_hints);
    for step in &pipe.steps {
        collect_v2_step_redaction_hints(step, hint, input_redaction_hints);
    }
}

pub(super) fn collect_v2_start_redaction_hints(
    start: &V2Start,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match start {
        V2Start::Ref(v2_ref) => collect_v2_ref_redaction_hint(v2_ref, hint, input_redaction_hints),
        V2Start::PipeValue | V2Start::ImplicitPipeValue => {
            hint.unknown_provenance = true;
        }
        V2Start::Literal(value) => collect_json_redaction_hints(value, hint, input_redaction_hints),
        V2Start::V1Expr(expr) => collect_expr_redaction_hints(expr, hint, input_redaction_hints),
    }
}

pub(super) fn collect_v2_step_redaction_hints(
    step: &V2Step,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match step {
        V2Step::Op(op) => {
            if op.op.starts_with('@') {
                collect_path_redaction_hint(&op.op, hint, input_redaction_hints);
            } else {
                hint.unknown_provenance = true;
            }
            for arg in &op.args {
                collect_v2_expr_redaction_hints(arg, hint, input_redaction_hints);
            }
        }
        V2Step::Object(object) => {
            hint.unknown_provenance = true;
            for field in &object.fields {
                match &field.value {
                    V2ObjectFieldValue::Expr(expr) => {
                        collect_v2_expr_redaction_hints(expr, hint, input_redaction_hints);
                    }
                    V2ObjectFieldValue::Value(value) => {
                        collect_json_redaction_hints(value, hint, input_redaction_hints);
                    }
                }
            }
        }
        V2Step::CustomCall(call) => {
            hint.unknown_provenance = true;
            if let Some(with) = &call.with {
                for (_, arg) in with {
                    match arg {
                        V2CallArg::Expr(expr) => {
                            collect_v2_expr_redaction_hints(expr, hint, input_redaction_hints);
                        }
                        V2CallArg::Value(value) => {
                            collect_json_redaction_hints(value, hint, input_redaction_hints);
                        }
                    }
                }
            }
        }
        V2Step::Let(let_step) => {
            for (_, expr) in &let_step.bindings {
                collect_v2_expr_redaction_hints(expr, hint, input_redaction_hints);
            }
        }
        V2Step::If(if_step) => {
            hint.unknown_provenance = true;
            collect_v2_condition_redaction_hints(&if_step.cond, hint, input_redaction_hints);
            collect_v2_pipe_redaction_hints(&if_step.then_branch, hint, input_redaction_hints);
            if let Some(else_branch) = &if_step.else_branch {
                collect_v2_pipe_redaction_hints(else_branch, hint, input_redaction_hints);
            }
        }
        V2Step::Map(map_step) => {
            hint.unknown_provenance = true;
            for step in &map_step.steps {
                collect_v2_step_redaction_hints(step, hint, input_redaction_hints);
            }
        }
        V2Step::Ref(v2_ref) => collect_v2_ref_redaction_hint(v2_ref, hint, input_redaction_hints),
    }
}

pub(super) fn collect_v2_condition_redaction_hints(
    condition: &V2Condition,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match condition {
        V2Condition::All(conditions) | V2Condition::Any(conditions) => {
            for condition in conditions {
                collect_v2_condition_redaction_hints(condition, hint, input_redaction_hints);
            }
        }
        V2Condition::Comparison(comparison) => {
            for arg in &comparison.args {
                collect_v2_expr_redaction_hints(arg, hint, input_redaction_hints);
            }
        }
        V2Condition::Expr(expr) => {
            collect_v2_expr_redaction_hints(expr, hint, input_redaction_hints);
        }
    }
}

pub(super) fn collect_v2_ref_redaction_hint(
    v2_ref: &V2Ref,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match v2_ref {
        V2Ref::Input(path) => {
            collect_path_redaction_hint(&canonical_input_path(path), hint, input_redaction_hints);
        }
        V2Ref::Context(path) => {
            collect_path_redaction_hint(&canonical_context_path(path), hint, input_redaction_hints);
        }
        V2Ref::Out(path) => {
            collect_path_redaction_hint(&canonical_out_path(path), hint, input_redaction_hints);
        }
        V2Ref::Pipe(_) => {
            hint.unknown_provenance = true;
        }
        V2Ref::Item(path) => {
            collect_path_redaction_hint(&canonical_item_path(path), hint, input_redaction_hints);
        }
        V2Ref::Acc(path) => {
            collect_path_redaction_hint(&canonical_acc_path(path), hint, input_redaction_hints);
        }
        V2Ref::Local(_) => {
            hint.unknown_provenance = true;
        }
    }
}
