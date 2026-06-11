use super::*;

pub(super) fn validate_step_call_sites(
    rule: &RuleFile,
    step: &V2Step,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
) {
    match step {
        V2Step::Op(op_step) => {
            if rule.defs.contains_key(&op_step.op) && !op_step.args.is_empty() {
                push_rule_error(
                    errors,
                    locator,
                    ErrorCode::InvalidArgs,
                    "custom op arguments must use with call options",
                    path,
                );
            }
            for (index, arg) in op_step.args.iter().enumerate() {
                validate_v2_expr_call_sites(
                    rule,
                    arg,
                    &format!("{}.args[{}]", path, index),
                    locator,
                    errors,
                    in_custom_body,
                );
            }
        }
        V2Step::Object(object_step) => {
            for field in &object_step.fields {
                if let V2ObjectFieldValue::Expr(expr) = &field.value {
                    validate_v2_expr_call_sites(
                        rule,
                        expr,
                        &object_field_rule_path(path, &field.key),
                        locator,
                        errors,
                        in_custom_body,
                    );
                }
            }
        }
        V2Step::CustomCall(call) => {
            validate_custom_call_site(rule, call, path, locator, errors, in_custom_body);
        }
        V2Step::Let(let_step) => {
            for (name, expr) in &let_step.bindings {
                validate_v2_expr_call_sites(
                    rule,
                    expr,
                    &format!("{}.let.{}", path, name),
                    locator,
                    errors,
                    in_custom_body,
                );
            }
        }
        V2Step::If(if_step) => {
            validate_condition_call_sites(
                rule,
                &if_step.cond,
                &format!("{}.if.cond", path),
                locator,
                errors,
                in_custom_body,
            );
            validate_pipe_call_sites(
                rule,
                &if_step.then_branch,
                &format!("{}.if.then", path),
                locator,
                errors,
                in_custom_body,
            );
            if let Some(else_branch) = &if_step.else_branch {
                validate_pipe_call_sites(
                    rule,
                    else_branch,
                    &format!("{}.if.else", path),
                    locator,
                    errors,
                    in_custom_body,
                );
            }
        }
        V2Step::Map(map_step) => {
            for (index, step) in map_step.steps.iter().enumerate() {
                validate_step_call_sites(
                    rule,
                    step,
                    &format!("{}.map[{}]", path, index),
                    locator,
                    errors,
                    in_custom_body,
                );
            }
        }
        V2Step::Ref(V2Ref::Context(_)) if in_custom_body => {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidRefNamespace,
                "@context is not available inside custom op bodies",
                path,
            );
        }
        V2Step::Ref(_) => {}
    }
}

pub(super) fn parse_known_custom_call_literal_start(
    rule: &RuleFile,
    start: &V2Start,
) -> Option<Result<V2CustomCallStep, crate::v2_parser::V2ParseError>> {
    let V2Start::Literal(value) = start else {
        return None;
    };
    let (op_name, args_val) = custom_call_step_candidate(value)?;
    if !rule.defs.contains_key(op_name) {
        return None;
    }
    match parse_custom_call_step(op_name, args_val) {
        Ok(Some(call)) => Some(Ok(call)),
        Ok(None) => Some(Err(crate::v2_parser::V2ParseError::InvalidStep(
            "custom op call must use with call options".to_string(),
        ))),
        Err(err) => Some(Err(err)),
    }
}

pub(super) fn validate_custom_call_site(
    rule: &RuleFile,
    call: &V2CustomCallStep,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
) {
    if !rule.defs.contains_key(&call.op) {
        push_rule_error(
            errors,
            locator,
            ErrorCode::UnknownOp,
            format!("unknown custom op: {}", call.op),
            path,
        );
    }
    if let Some(def) = rule.defs.get(&call.op)
        && let Some(with) = &call.with
    {
        validate_with_shape(&def.input, with, path, locator, errors);
        for (name, arg) in with {
            if let V2CallArg::Expr(expr) = arg {
                validate_v2_expr_call_sites(
                    rule,
                    expr,
                    &format!("{}.with.{}", path, name),
                    locator,
                    errors,
                    in_custom_body,
                );
            }
        }
    }
}

pub(super) fn validate_condition_call_sites(
    rule: &RuleFile,
    condition: &V2Condition,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
) {
    match condition {
        V2Condition::All(items) | V2Condition::Any(items) => {
            for (index, item) in items.iter().enumerate() {
                validate_condition_call_sites(
                    rule,
                    item,
                    &format!("{}[{}]", path, index),
                    locator,
                    errors,
                    in_custom_body,
                );
            }
        }
        V2Condition::Comparison(comparison) => {
            for (index, arg) in comparison.args.iter().enumerate() {
                validate_v2_expr_call_sites(
                    rule,
                    arg,
                    &format!("{}.args[{}]", path, index),
                    locator,
                    errors,
                    in_custom_body,
                );
            }
        }
        V2Condition::Expr(expr) => {
            validate_v2_expr_call_sites(rule, expr, path, locator, errors, in_custom_body)
        }
    }
}

pub(super) fn validate_with_shape(
    input: &RuleType,
    with: &[(String, V2CallArg)],
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    let fields = match &input.kind {
        RuleTypeKind::Object(fields) => fields,
        RuleTypeKind::Json => return,
        _ => {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidArgs,
                "with adapter requires object input",
                path,
            );
            return;
        }
    };
    let with_keys: HashSet<&str> = with.iter().map(|(key, _)| key.as_str()).collect();
    for key in with_keys {
        if !fields.contains_key(key) {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidArgs,
                format!("with adapter contains unknown field `{}`", key),
                path,
            );
        }
    }
    for (key, field) in fields {
        if !field.optional && !with.iter().any(|(with_key, _)| with_key == key) {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidArgs,
                format!("with adapter missing required field `{}`", key),
                path,
            );
        }
    }
}
