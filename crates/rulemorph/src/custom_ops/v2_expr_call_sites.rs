use super::*;

pub(super) fn validate_expr_call_sites(
    rule: &RuleFile,
    expr: &crate::model::Expr,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
    produced_targets: &HashSet<Vec<PathToken>>,
) {
    let Some(value) = crate::expr_json::expr_to_json_for_v2_pipe(expr) else {
        if in_custom_body {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidExprShape,
                "custom op expr must be a v2 pipe",
                path,
            );
        }
        return;
    };
    let v2_expr = match parse_v2_expr(&value) {
        Ok(expr) => expr,
        Err(err) => {
            if in_custom_body {
                push_rule_error(
                    errors,
                    locator,
                    ErrorCode::InvalidExprShape,
                    format!("invalid v2 expression: {:?}", err),
                    path,
                );
            }
            return;
        }
    };
    if in_custom_body {
        validate_v2_expr_standard_rules(rule, &v2_expr, path, locator, errors, produced_targets);
    };
    validate_v2_expr_call_sites(rule, &v2_expr, path, locator, errors, in_custom_body);
}

pub(super) fn validate_condition_expr_call_sites(
    rule: &RuleFile,
    expr: &crate::model::Expr,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
    produced_targets: &HashSet<Vec<PathToken>>,
) {
    let Some(value) = crate::expr_json::expr_to_json_for_v2_condition(expr) else {
        if in_custom_body {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidExprShape,
                "custom op condition must be a v2 condition",
                path,
            );
        }
        return;
    };
    let condition = match parse_v2_condition(&value) {
        Ok(condition) => condition,
        Err(err) => {
            if in_custom_body {
                push_rule_error(
                    errors,
                    locator,
                    ErrorCode::InvalidExprShape,
                    format!("invalid v2 condition: {:?}", err),
                    path,
                );
            }
            return;
        }
    };
    if in_custom_body {
        validate_v2_condition_standard_rules(
            rule,
            &condition,
            path,
            locator,
            errors,
            produced_targets,
        );
    }
    validate_condition_call_sites(rule, &condition, path, locator, errors, in_custom_body);
}

pub(super) fn validate_v2_expr_standard_rules(
    rule: &RuleFile,
    expr: &V2Expr,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    produced_targets: &HashSet<Vec<PathToken>>,
) {
    let mut ctx = V2ValidationCtx::with_produced_targets(locator, produced_targets.clone(), false)
        .with_custom_op_names(rule.defs.keys().cloned().collect());
    validate_v2_expr_rules(expr, path, &V2Scope::new().with_pipe(), &mut ctx);
    errors.extend(ctx.errors().iter().cloned());
}

pub(super) fn validate_v2_condition_standard_rules(
    rule: &RuleFile,
    condition: &V2Condition,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    produced_targets: &HashSet<Vec<PathToken>>,
) {
    let mut ctx = V2ValidationCtx::with_produced_targets(locator, produced_targets.clone(), false)
        .with_custom_op_names(rule.defs.keys().cloned().collect());
    validate_v2_condition(condition, path, &V2Scope::new().with_pipe(), &mut ctx);
    errors.extend(ctx.errors().iter().cloned());
}

pub(super) fn validate_v2_expr_call_sites(
    rule: &RuleFile,
    expr: &V2Expr,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
) {
    let V2Expr::Pipe(pipe) = expr else {
        return;
    };
    validate_pipe_call_sites(rule, pipe, path, locator, errors, in_custom_body);
}

pub(super) fn validate_pipe_call_sites(
    rule: &RuleFile,
    pipe: &V2Pipe,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
) {
    if in_custom_body {
        validate_start_no_forbidden_capture(&pipe.start, path, locator, errors);
    }
    if let Some(call) = parse_known_custom_call_literal_start(rule, &pipe.start) {
        let start_path = format!("{}[0]", path);
        match call {
            Ok(call) => {
                validate_custom_call_site(rule, &call, &start_path, locator, errors, in_custom_body)
            }
            Err(err) => push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidExprShape,
                format!("invalid custom op call: {}", err),
                &start_path,
            ),
        }
    }
    for (index, step) in pipe.steps.iter().enumerate() {
        validate_step_call_sites(
            rule,
            step,
            &format!("{}[{}]", path, index + 1),
            locator,
            errors,
            in_custom_body,
        );
    }
}

pub(super) fn validate_start_no_forbidden_capture(
    start: &V2Start,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    if let V2Start::Ref(V2Ref::Context(_)) = start {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidRefNamespace,
            "@context is not available inside custom op bodies",
            path,
        );
    }
}
