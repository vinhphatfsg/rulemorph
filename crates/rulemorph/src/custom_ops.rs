use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::error::{ErrorCode, RuleError, TransformError, TransformErrorKind};
use crate::locator::YamlLocator;
use crate::model::{CustomOpDef, Mapping, RuleFile, RuleType, RuleTypeField, RuleTypeKind};
use crate::path::{PathToken, parse_path};
use crate::v2_model::{
    V2CallArg, V2Condition, V2CustomCallStep, V2Expr, V2Pipe, V2Ref, V2Start, V2Step,
};
use crate::v2_operator::is_valid_operator;
use crate::v2_parser::{
    custom_call_step_candidate, parse_custom_call_step, parse_v2_condition, parse_v2_expr,
};
use crate::v2_validator::{
    V2Scope, V2ValidationCtx, validate_v2_condition, validate_v2_expr as validate_v2_expr_rules,
};

pub(crate) const MAX_DEFS: usize = 128;
pub(crate) const MAX_CUSTOM_OP_BODY_NODES: usize = 2048;
pub(crate) const MAX_CUSTOM_OP_CALL_DEPTH: usize = 64;
pub(crate) const MAX_CUSTOM_OP_CALLS_PER_RECORD: usize = 100_000;
pub(crate) const MAX_TYPE_DEPTH: usize = 32;
pub(crate) const MAX_TYPE_FIELDS: usize = 512;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ContractMode {
    InputWidth,
    AdapterExact,
    OutputExact,
}

pub(crate) fn validate_defs(
    rule: &RuleFile,
    locator: Option<&YamlLocator>,
) -> Result<(), Vec<RuleError>> {
    let mut errors = Vec::new();
    if !rule.defs.is_empty() && rule.version != 2 {
        push_rule_error(
            &mut errors,
            locator,
            ErrorCode::InvalidStep,
            "defs is only supported in version 2",
            "defs",
        );
        return Err(errors);
    }

    if rule.defs.len() > MAX_DEFS {
        push_rule_error(
            &mut errors,
            locator,
            ErrorCode::InvalidStep,
            "custom op defs exceed configured limit",
            "defs",
        );
    }

    for (name, def) in &rule.defs {
        let def_path = format!("defs.{}", name);
        if !is_valid_custom_op_name(name) {
            push_rule_error(
                &mut errors,
                locator,
                ErrorCode::InvalidStep,
                format!("custom op name `{}` is invalid", name),
                &def_path,
            );
        }
        if is_reserved_or_builtin_custom_op_name(name) {
            push_rule_error(
                &mut errors,
                locator,
                ErrorCode::UnknownOp,
                format!(
                    "custom op `{}` must not shadow a built-in or reserved op",
                    name
                ),
                &def_path,
            );
        }
        validate_type_limits(
            &def.input,
            &format!("{}.input", def_path),
            locator,
            &mut errors,
        );
        if let Some(returns) = &def.returns {
            validate_type_limits(
                returns,
                &format!("{}.returns", def_path),
                locator,
                &mut errors,
            );
        }
        match (&def.expr, &def.mappings) {
            (Some(_), Some(_)) => push_rule_error(
                &mut errors,
                locator,
                ErrorCode::InvalidStep,
                "custom op must define only one of expr or mappings",
                &def_path,
            ),
            (None, None) => push_rule_error(
                &mut errors,
                locator,
                ErrorCode::InvalidStep,
                "custom op must define expr or mappings",
                &def_path,
            ),
            (Some(_), None) if def.returns.is_none() => push_rule_error(
                &mut errors,
                locator,
                ErrorCode::InvalidStep,
                "custom op expr body requires returns",
                &format!("{}.returns", def_path),
            ),
            _ => {}
        }

        let nodes = custom_op_body_nodes(def);
        if nodes > MAX_CUSTOM_OP_BODY_NODES {
            push_rule_error(
                &mut errors,
                locator,
                ErrorCode::InvalidStep,
                "custom op body exceeds configured node limit",
                &def_path,
            );
        }

        validate_custom_mappings_shape(def, &def_path, locator, &mut errors);
    }

    validate_dependency_cycles(rule, locator, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

pub(crate) fn validate_custom_call_sites(
    rule: &RuleFile,
    locator: Option<&YamlLocator>,
) -> Vec<RuleError> {
    let mut errors = Vec::new();
    for (name, def) in &rule.defs {
        let base = format!("defs.{}", name);
        validate_def_body_call_sites(rule, def, &base, locator, &mut errors, true);
    }
    for (index, mapping) in rule.mappings.iter().enumerate() {
        validate_mapping_call_sites(
            rule,
            mapping,
            &format!("mappings[{}]", index),
            locator,
            &mut errors,
            false,
            &HashSet::new(),
        );
    }
    if let Some(expr) = &rule.record_when {
        validate_condition_expr_call_sites(
            rule,
            expr,
            "record_when",
            locator,
            &mut errors,
            false,
            &HashSet::new(),
        );
    }
    if let Some(steps) = &rule.steps {
        for (step_index, step) in steps.iter().enumerate() {
            let base = format!("steps[{}]", step_index);
            if let Some(mappings) = &step.mappings {
                for (index, mapping) in mappings.iter().enumerate() {
                    validate_mapping_call_sites(
                        rule,
                        mapping,
                        &format!("{}.mappings[{}]", base, index),
                        locator,
                        &mut errors,
                        false,
                        &HashSet::new(),
                    );
                }
            }
            if let Some(expr) = &step.record_when {
                validate_condition_expr_call_sites(
                    rule,
                    expr,
                    &format!("{}.record_when", base),
                    locator,
                    &mut errors,
                    false,
                    &HashSet::new(),
                );
            }
            if let Some(asserts) = &step.asserts {
                for (index, assert) in asserts.iter().enumerate() {
                    validate_condition_expr_call_sites(
                        rule,
                        &assert.when,
                        &format!("{}.asserts[{}].when", base, index),
                        locator,
                        &mut errors,
                        false,
                        &HashSet::new(),
                    );
                }
            }
            if let Some(branch) = &step.branch {
                validate_condition_expr_call_sites(
                    rule,
                    &branch.when,
                    &format!("{}.branch.when", base),
                    locator,
                    &mut errors,
                    false,
                    &HashSet::new(),
                );
            }
        }
    }
    if let Some(finalize) = &rule.finalize {
        if let Some(filter) = &finalize.filter {
            validate_condition_expr_call_sites(
                rule,
                filter,
                "finalize.filter",
                locator,
                &mut errors,
                false,
                &HashSet::new(),
            );
        }
        if let Some(wrap) = &finalize.wrap {
            validate_finalize_wrap_call_sites(rule, wrap, "finalize.wrap", locator, &mut errors);
        }
    }
    errors
}

fn validate_finalize_wrap_call_sites(
    rule: &RuleFile,
    value: &JsonValue,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    match value {
        JsonValue::Object(map) => {
            for (key, value) in map {
                validate_finalize_wrap_call_sites(
                    rule,
                    value,
                    &format!("{}.{}", path, key),
                    locator,
                    errors,
                );
            }
        }
        _ => {
            let Ok(expr) = parse_v2_expr(value) else {
                return;
            };
            validate_v2_expr_call_sites(rule, &expr, path, locator, errors, false);
        }
    }
}

fn validate_mapping_call_sites(
    rule: &RuleFile,
    mapping: &Mapping,
    base_path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
    produced_targets: &HashSet<Vec<PathToken>>,
) {
    if let Some(expr) = &mapping.expr {
        validate_expr_call_sites(
            rule,
            expr,
            &format!("{}.expr", base_path),
            locator,
            errors,
            in_custom_body,
            produced_targets,
        );
    }
    if let Some(when) = &mapping.when {
        validate_condition_expr_call_sites(
            rule,
            when,
            &format!("{}.when", base_path),
            locator,
            errors,
            in_custom_body,
            produced_targets,
        );
    }
}

fn validate_def_body_call_sites(
    rule: &RuleFile,
    def: &CustomOpDef,
    base_path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
) {
    if let Some(expr) = &def.expr {
        validate_expr_call_sites(
            rule,
            expr,
            &format!("{}.expr", base_path),
            locator,
            errors,
            in_custom_body,
            &HashSet::new(),
        );
    }
    if let Some(mappings) = &def.mappings {
        let mut produced_targets = HashSet::new();
        for (index, mapping) in mappings.iter().enumerate() {
            validate_mapping_call_sites(
                rule,
                mapping,
                &format!("{}.mappings[{}]", base_path, index),
                locator,
                errors,
                in_custom_body,
                &produced_targets,
            );
            if let Ok(tokens) = parse_path(&mapping.target) {
                produced_targets.insert(tokens);
            }
        }
    }
}

fn validate_custom_mappings_shape(
    def: &CustomOpDef,
    base_path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    let Some(mappings) = &def.mappings else {
        return;
    };

    if let Some(returns) = &def.returns
        && !matches!(returns.kind, RuleTypeKind::Json | RuleTypeKind::Object(_))
    {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidTypeName,
            "custom op mappings body returns must be object or json",
            &format!("{}.returns", base_path),
        );
    }

    let mut produced_targets: HashSet<Vec<PathToken>> = HashSet::new();
    for (index, mapping) in mappings.iter().enumerate() {
        let base = format!("{}.mappings[{}]", base_path, index);

        if mapping.target.trim().is_empty() {
            push_rule_error(
                errors,
                locator,
                ErrorCode::MissingTarget,
                "mapping.target is required",
                &format!("{}.target", base),
            );
        }

        let target_tokens = match parse_path(&mapping.target) {
            Ok(tokens) => tokens,
            Err(_) => {
                push_rule_error(
                    errors,
                    locator,
                    ErrorCode::InvalidPath,
                    "target path is invalid",
                    &format!("{}.target", base),
                );
                continue;
            }
        };

        if target_tokens
            .iter()
            .any(|token| matches!(token, PathToken::Index(_)))
        {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidPath,
                "target path must not include indexes",
                &format!("{}.target", base),
            );
            continue;
        }

        if produced_targets.contains(&target_tokens) {
            push_rule_error(
                errors,
                locator,
                ErrorCode::DuplicateTarget,
                "mapping.target is duplicated",
                &format!("{}.target", base),
            );
        }

        let value_count = custom_mapping_value_count(mapping);
        if value_count == 0 {
            push_rule_error(
                errors,
                locator,
                ErrorCode::MissingMappingValue,
                "mapping must define source, value, or expr",
                &base,
            );
        } else if value_count > 1 {
            push_rule_error(
                errors,
                locator,
                ErrorCode::SourceValueExprExclusive,
                "exactly one of source/value/expr is required",
                &base,
            );
        }

        if let Some(type_name) = &mapping.value_type
            && !matches!(type_name.as_str(), "string" | "int" | "float" | "bool")
        {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidTypeName,
                "type must be string|int|float|bool",
                &format!("{}.type", base),
            );
        }

        if let Some(source) = &mapping.source {
            validate_custom_mapping_source(source, &base, &produced_targets, locator, errors);
        }

        produced_targets.insert(target_tokens);
    }
}

fn custom_mapping_value_count(mapping: &Mapping) -> usize {
    usize::from(mapping.source.is_some())
        + usize::from(mapping.value.is_some())
        + usize::from(mapping.expr.is_some())
}

fn validate_custom_mapping_source(
    source: &str,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    let full_path = format!("{}.source", base_path);
    let Some((namespace, path)) = parse_mapping_source_ref(source) else {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidRefNamespace,
            "ref namespace must be input|out",
            &full_path,
        );
        return;
    };

    if namespace == CustomSourceNamespace::Context {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidRefNamespace,
            "@context is not available inside custom op bodies",
            &full_path,
        );
        return;
    }

    let tokens = match parse_path(path) {
        Ok(tokens) => tokens,
        Err(_) => {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidPath,
                "path is invalid",
                &full_path,
            );
            return;
        }
    };

    if namespace == CustomSourceNamespace::Out && !out_ref_resolves(&tokens, produced_targets) {
        push_rule_error(
            errors,
            locator,
            ErrorCode::ForwardOutReference,
            "out reference must point to previous mappings",
            &full_path,
        );
    }
}

fn parse_mapping_source_ref(value: &str) -> Option<(CustomSourceNamespace, &str)> {
    if let Some((prefix, path)) = value.split_once('.') {
        if path.is_empty() {
            return None;
        }
        let namespace = match prefix {
            "input" => CustomSourceNamespace::Input,
            "context" => CustomSourceNamespace::Context,
            "out" => CustomSourceNamespace::Out,
            _ => return None,
        };
        Some((namespace, path))
    } else {
        if value.is_empty() {
            return None;
        }
        Some((CustomSourceNamespace::Input, value))
    }
}

fn out_ref_resolves(tokens: &[PathToken], produced_targets: &HashSet<Vec<PathToken>>) -> bool {
    if !tokens
        .iter()
        .any(|token| matches!(token, PathToken::Key(_)))
    {
        return false;
    }

    for produced in produced_targets {
        if is_path_prefix(produced, tokens) || is_path_prefix(tokens, produced) {
            return true;
        }
    }
    false
}

fn is_path_prefix(prefix: &[PathToken], tokens: &[PathToken]) -> bool {
    prefix.len() <= tokens.len() && prefix.iter().zip(tokens).all(|(left, right)| left == right)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CustomSourceNamespace {
    Input,
    Context,
    Out,
}

fn validate_expr_call_sites(
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

fn validate_condition_expr_call_sites(
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

fn validate_v2_expr_standard_rules(
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

fn validate_v2_condition_standard_rules(
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

fn validate_v2_expr_call_sites(
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

fn validate_pipe_call_sites(
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

fn validate_start_no_forbidden_capture(
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

fn validate_step_call_sites(
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

fn parse_known_custom_call_literal_start(
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

fn validate_custom_call_site(
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

fn validate_condition_call_sites(
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

fn validate_with_shape(
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

fn validate_dependency_cycles(
    rule: &RuleFile,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    let mut graph: HashMap<String, HashSet<String>> = HashMap::new();
    for (name, def) in &rule.defs {
        let mut deps = HashSet::new();
        collect_def_dependencies(rule, def, &mut deps);
        graph.insert(name.clone(), deps);
    }
    let mut visited = HashSet::new();
    let mut stack = Vec::new();
    for name in rule.defs.keys() {
        if detects_cycle(name, &graph, &mut visited, &mut stack) {
            push_rule_error(
                errors,
                locator,
                ErrorCode::CyclicDependency,
                format!("cyclic custom op dependency involving `{}`", name),
                &format!("defs.{}", name),
            );
        }
    }
}

fn collect_def_dependencies(rule: &RuleFile, def: &CustomOpDef, deps: &mut HashSet<String>) {
    if let Some(expr) = &def.expr
        && let Some(value) = crate::expr_json::expr_to_json_for_v2_pipe(expr)
        && let Ok(expr) = parse_v2_expr(&value)
    {
        collect_expr_dependencies(rule, &expr, deps);
    }
    if let Some(mappings) = &def.mappings {
        for mapping in mappings {
            if let Some(expr) = &mapping.expr
                && let Some(value) = crate::expr_json::expr_to_json_for_v2_pipe(expr)
                && let Ok(expr) = parse_v2_expr(&value)
            {
                collect_expr_dependencies(rule, &expr, deps);
            }
            if let Some(when) = &mapping.when
                && let Some(value) = crate::expr_json::expr_to_json_for_v2_condition(when)
                && let Ok(condition) = parse_v2_condition(&value)
            {
                collect_condition_dependencies(rule, &condition, deps);
            }
        }
    }
}

fn collect_expr_dependencies(rule: &RuleFile, expr: &V2Expr, deps: &mut HashSet<String>) {
    let V2Expr::Pipe(pipe) = expr else {
        return;
    };
    collect_pipe_dependencies(rule, pipe, deps);
}

fn collect_step_dependencies(rule: &RuleFile, step: &V2Step, deps: &mut HashSet<String>) {
    match step {
        V2Step::Op(op) => {
            if rule.defs.contains_key(&op.op) {
                deps.insert(op.op.clone());
            }
            for arg in &op.args {
                collect_expr_dependencies(rule, arg, deps);
            }
        }
        V2Step::CustomCall(call) => {
            collect_custom_call_dependencies(rule, call, deps);
        }
        V2Step::Let(let_step) => {
            for (_, expr) in &let_step.bindings {
                collect_expr_dependencies(rule, expr, deps);
            }
        }
        V2Step::If(if_step) => {
            collect_condition_dependencies(rule, &if_step.cond, deps);
            collect_pipe_dependencies(rule, &if_step.then_branch, deps);
            if let Some(else_branch) = &if_step.else_branch {
                collect_pipe_dependencies(rule, else_branch, deps);
            }
        }
        V2Step::Map(map_step) => {
            for step in &map_step.steps {
                collect_step_dependencies(rule, step, deps);
            }
        }
        V2Step::Ref(_) => {}
    }
}

fn collect_pipe_dependencies(rule: &RuleFile, pipe: &V2Pipe, deps: &mut HashSet<String>) {
    if let Some(Ok(call)) = parse_known_custom_call_literal_start(rule, &pipe.start) {
        collect_custom_call_dependencies(rule, &call, deps);
    }
    for step in &pipe.steps {
        collect_step_dependencies(rule, step, deps);
    }
}

fn collect_custom_call_dependencies(
    rule: &RuleFile,
    call: &V2CustomCallStep,
    deps: &mut HashSet<String>,
) {
    if rule.defs.contains_key(&call.op) {
        deps.insert(call.op.clone());
    }
    if let Some(with) = &call.with {
        for (_, arg) in with {
            if let V2CallArg::Expr(expr) = arg {
                collect_expr_dependencies(rule, expr, deps);
            }
        }
    }
}

fn collect_condition_dependencies(
    rule: &RuleFile,
    condition: &V2Condition,
    deps: &mut HashSet<String>,
) {
    match condition {
        V2Condition::All(items) | V2Condition::Any(items) => {
            for item in items {
                collect_condition_dependencies(rule, item, deps);
            }
        }
        V2Condition::Comparison(comparison) => {
            for arg in &comparison.args {
                collect_expr_dependencies(rule, arg, deps);
            }
        }
        V2Condition::Expr(expr) => collect_expr_dependencies(rule, expr, deps),
    }
}

fn detects_cycle(
    name: &str,
    graph: &HashMap<String, HashSet<String>>,
    visited: &mut HashSet<String>,
    stack: &mut Vec<String>,
) -> bool {
    if stack.iter().any(|entry| entry == name) {
        return true;
    }
    if !visited.insert(name.to_string()) {
        return false;
    }
    stack.push(name.to_string());
    if let Some(deps) = graph.get(name) {
        for dep in deps {
            if detects_cycle(dep, graph, visited, stack) {
                return true;
            }
        }
    }
    stack.pop();
    false
}

pub(crate) fn check_contract(
    value: &JsonValue,
    ty: &RuleType,
    mode: ContractMode,
    path: &str,
) -> Result<(), TransformError> {
    check_contract_inner(value, ty, mode, path).map_err(|message| {
        TransformError::new(TransformErrorKind::ExprError, message).with_path(path)
    })
}

fn check_contract_inner(
    value: &JsonValue,
    ty: &RuleType,
    mode: ContractMode,
    path: &str,
) -> Result<(), String> {
    if matches!(ty.kind, RuleTypeKind::Json) {
        return Ok(());
    }
    if value.is_null() {
        return if ty.nullable {
            Ok(())
        } else {
            Err(format!("{} expected {}, got null", path, type_name(ty)))
        };
    }
    match &ty.kind {
        RuleTypeKind::String => value
            .is_string()
            .then_some(())
            .ok_or_else(|| format!("{} expected string, got {}", path, json_type(value))),
        RuleTypeKind::Int => value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
            .map(|_| ())
            .ok_or_else(|| format!("{} expected int, got {}", path, json_type(value))),
        RuleTypeKind::Float | RuleTypeKind::Number => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(|_| ())
            .ok_or_else(|| format!("{} expected number, got {}", path, json_type(value))),
        RuleTypeKind::Bool => value
            .is_boolean()
            .then_some(())
            .ok_or_else(|| format!("{} expected bool, got {}", path, json_type(value))),
        RuleTypeKind::Json => Ok(()),
        RuleTypeKind::Array(item_ty) => {
            let JsonValue::Array(items) = value else {
                return Err(format!("{} expected array, got {}", path, json_type(value)));
            };
            for (index, item) in items.iter().enumerate() {
                check_contract_inner(item, item_ty, mode, &format!("{}[{}]", path, index))?;
            }
            Ok(())
        }
        RuleTypeKind::Object(fields) => check_object_contract(value, fields, mode, path),
    }
}

fn check_object_contract(
    value: &JsonValue,
    fields: &BTreeMap<String, RuleTypeField>,
    mode: ContractMode,
    path: &str,
) -> Result<(), String> {
    let JsonValue::Object(object) = value else {
        return Err(format!(
            "{} expected object, got {}",
            path,
            json_type(value)
        ));
    };
    for (name, field) in fields {
        match object.get(name) {
            Some(value) => {
                check_contract_inner(value, &field.ty, mode, &format!("{}.{}", path, name))?
            }
            None if field.optional => {}
            None => return Err(format!("{} missing required field `{}`", path, name)),
        }
    }
    if matches!(mode, ContractMode::AdapterExact | ContractMode::OutputExact) {
        for name in object.keys() {
            if !fields.contains_key(name) {
                return Err(format!("{} contains unexpected field `{}`", path, name));
            }
        }
    }
    Ok(())
}

pub(crate) fn build_with_object(items: impl IntoIterator<Item = (String, JsonValue)>) -> JsonValue {
    let mut object = JsonMap::new();
    for (key, value) in items {
        object.insert(key, value);
    }
    JsonValue::Object(object)
}

fn validate_type_limits(
    ty: &RuleType,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    let mut fields = 0usize;
    let depth = type_depth_and_fields(ty, &mut fields);
    if depth > MAX_TYPE_DEPTH {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidTypeName,
            "custom op type exceeds configured depth limit",
            path,
        );
    }
    if fields > MAX_TYPE_FIELDS {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidTypeName,
            "custom op type exceeds configured field limit",
            path,
        );
    }
}

fn type_depth_and_fields(ty: &RuleType, fields: &mut usize) -> usize {
    match &ty.kind {
        RuleTypeKind::Array(item) => 1 + type_depth_and_fields(item, fields),
        RuleTypeKind::Object(map) => {
            *fields += map.len();
            1 + map
                .values()
                .map(|field| type_depth_and_fields(&field.ty, fields))
                .max()
                .unwrap_or(0)
        }
        _ => 1,
    }
}

fn custom_op_body_nodes(def: &CustomOpDef) -> usize {
    let mut count = 0usize;
    if let Some(expr) = &def.expr {
        count += expr_node_count(expr);
    }
    if let Some(mappings) = &def.mappings {
        count += mappings.len();
        for mapping in mappings {
            if let Some(expr) = &mapping.expr {
                count += expr_node_count(expr);
            }
            if let Some(when) = &mapping.when {
                count += condition_node_count(when);
            }
            if let Some(value) = &mapping.value {
                count += json_node_count(value);
            }
            if let Some(default) = &mapping.default {
                count += json_node_count(default);
            }
        }
    }
    count
}

fn expr_node_count(expr: &crate::model::Expr) -> usize {
    let Some(value) = crate::expr_json::expr_to_json_for_v2_pipe(expr) else {
        return 1;
    };
    json_node_count(&value)
}

fn condition_node_count(expr: &crate::model::Expr) -> usize {
    let Some(value) = crate::expr_json::expr_to_json_for_v2_condition(expr) else {
        return 1;
    };
    json_node_count(&value)
}

fn json_node_count(value: &JsonValue) -> usize {
    match value {
        JsonValue::Array(items) => 1 + items.iter().map(json_node_count).sum::<usize>(),
        JsonValue::Object(map) => 1 + map.values().map(json_node_count).sum::<usize>(),
        _ => 1,
    }
}

fn is_valid_custom_op_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub(crate) fn is_reserved_or_builtin_custom_op_name(name: &str) -> bool {
    is_valid_operator(name) || is_reserved_custom_op_name(name)
}

fn is_reserved_custom_op_name(name: &str) -> bool {
    matches!(
        name,
        "op" | "let" | "if" | "map" | "then" | "else" | "cond" | "ref"
    )
}

fn push_rule_error(
    errors: &mut Vec<RuleError>,
    locator: Option<&YamlLocator>,
    code: ErrorCode,
    message: impl Into<String>,
    path: &str,
) {
    let mut err = RuleError::new(code, message).with_path(path);
    if let Some(locator) = locator
        && let Some(location) = locator.location_for(path)
    {
        err = err.with_location(location.line, location.column);
    }
    errors.push(err);
}

fn type_name(ty: &RuleType) -> &'static str {
    match &ty.kind {
        RuleTypeKind::String => "string",
        RuleTypeKind::Int => "int",
        RuleTypeKind::Float => "float",
        RuleTypeKind::Number => "number",
        RuleTypeKind::Bool => "bool",
        RuleTypeKind::Json => "json",
        RuleTypeKind::Array(_) => "array",
        RuleTypeKind::Object(_) => "object",
    }
}

fn json_type(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}
