use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use axum::http::Method;
use rulemorph::serde_guard::parse_yaml_value_strict;
use rulemorph::v2_parser::parse_v2_expr;
use rulemorph::{RuleFormat, parse_rule_file_with_format, validate_rule_file_with_source};
use serde::de::DeserializeOwned;

use super::{
    CompiledEndpointRule, EndpointRuleFile, NetworkRuleFile, compile_retry, parse_duration,
    resolve_rule_path,
};

mod diagnostics;

pub use self::diagnostics::{RulesDirError, RulesDirErrors};
use self::diagnostics::{push_error, push_parse_error, push_rule_error};

#[derive(Debug, Default, Clone, Copy)]
struct RuleRefUsage {
    step: bool,
    body_rule: bool,
    catch_rule: bool,
    branch_rule: bool,
}

impl RuleRefUsage {
    fn step() -> Self {
        RuleRefUsage {
            step: true,
            ..RuleRefUsage::default()
        }
    }

    fn body_rule() -> Self {
        RuleRefUsage {
            body_rule: true,
            ..RuleRefUsage::default()
        }
    }

    fn catch_rule() -> Self {
        RuleRefUsage {
            catch_rule: true,
            ..RuleRefUsage::default()
        }
    }

    fn branch_rule() -> Self {
        RuleRefUsage {
            branch_rule: true,
            ..RuleRefUsage::default()
        }
    }

    fn merge(&mut self, other: RuleRefUsage) {
        self.step |= other.step;
        self.body_rule |= other.body_rule;
        self.catch_rule |= other.catch_rule;
        self.branch_rule |= other.branch_rule;
    }
}

#[derive(Debug, Default)]
struct ValidationState {
    validated_content: BTreeSet<PathBuf>,
}

pub fn validate_rules_dir(rules_dir: &Path) -> std::result::Result<(), RulesDirErrors> {
    let mut errors = Vec::new();
    let endpoint_path = rules_dir.join("endpoint.yaml");
    let source = match read_rule_source(&endpoint_path, &mut errors) {
        Some(source) => source,
        None => return Err(RulesDirErrors { errors }),
    };

    let raw: EndpointRuleFile = match parse_yaml(&endpoint_path, &source, &mut errors) {
        Some(raw) => raw,
        None => return Err(RulesDirErrors { errors }),
    };

    if raw.version != 2 {
        push_error(
            &mut errors,
            "InvalidVersion",
            &endpoint_path,
            "endpoint rule version must be 2",
            Some("version".to_string()),
            None,
        );
    }
    if raw.rule_type != "endpoint" {
        push_error(
            &mut errors,
            "InvalidRuleType",
            &endpoint_path,
            "endpoint rule type must be endpoint",
            Some("type".to_string()),
            None,
        );
    }
    if let Err(err) = CompiledEndpointRule::compile(raw.clone(), &endpoint_path) {
        push_error(
            &mut errors,
            "EndpointCompileFailed",
            &endpoint_path,
            err.to_string(),
            None,
            None,
        );
    }

    let base_dir = endpoint_path.parent().unwrap_or_else(|| Path::new("."));
    let mut refs: BTreeSet<PathBuf> = BTreeSet::new();
    let mut ref_usage: HashMap<PathBuf, RuleRefUsage> = HashMap::new();
    for endpoint in &raw.endpoints {
        for step in &endpoint.steps {
            let resolved = resolve_rule_path(base_dir, &step.rule);
            refs.insert(resolved.clone());
            ref_usage
                .entry(resolved)
                .and_modify(|usage| usage.merge(RuleRefUsage::step()))
                .or_insert_with(RuleRefUsage::step);
            if let Some(catch) = &step.catch {
                for target in catch.values() {
                    let resolved = resolve_rule_path(base_dir, target);
                    refs.insert(resolved.clone());
                    ref_usage
                        .entry(resolved)
                        .and_modify(|usage| usage.merge(RuleRefUsage::catch_rule()))
                        .or_insert_with(RuleRefUsage::catch_rule);
                }
            }
        }
        if let Some(catch) = &endpoint.catch {
            for target in catch.values() {
                let resolved = resolve_rule_path(base_dir, target);
                refs.insert(resolved.clone());
                ref_usage
                    .entry(resolved)
                    .and_modify(|usage| usage.merge(RuleRefUsage::catch_rule()))
                    .or_insert_with(RuleRefUsage::catch_rule);
            }
        }
    }

    let mut state = ValidationState::default();
    for path in refs {
        let usage = ref_usage.get(&path).copied().unwrap_or_default();
        validate_rule_path(&path, usage, &mut state, &mut errors);
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(RulesDirErrors { errors })
    }
}

fn read_rule_source(path: &Path, errors: &mut Vec<RulesDirError>) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(source) => Some(source),
        Err(err) => {
            push_error(errors, "ReadFailed", path, err.to_string(), None, None);
            None
        }
    }
}

fn parse_yaml<T: DeserializeOwned>(
    path: &Path,
    source: &str,
    errors: &mut Vec<RulesDirError>,
) -> Option<T> {
    let value = match parse_yaml_value_strict(source) {
        Ok(value) => value,
        Err(err) => {
            push_parse_error(errors, path, &err.to_string(), err.location());
            return None;
        }
    };
    match serde_yaml::from_value(value) {
        Ok(value) => Some(value),
        Err(err) => {
            let location = err.location().map(|loc| (loc.line(), loc.column()));
            push_parse_error(errors, path, &err.to_string(), location);
            None
        }
    }
}

fn parse_rule_type(path: &Path, source: &str, errors: &mut Vec<RulesDirError>) -> Option<String> {
    let meta: serde_yaml::Value = match parse_yaml_value_strict(source) {
        Ok(value) => value,
        Err(err) => {
            push_parse_error(errors, path, &err.to_string(), err.location());
            return None;
        }
    };
    Some(
        meta.get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("normal")
            .to_string(),
    )
}

fn validate_rule_path(
    path: &Path,
    usage: RuleRefUsage,
    state: &mut ValidationState,
    errors: &mut Vec<RulesDirError>,
) {
    let source = match read_rule_source(path, errors) {
        Some(source) => source,
        None => return,
    };
    let rule_type = match parse_rule_type(path, &source, errors) {
        Some(rule_type) => rule_type,
        None => return,
    };

    if usage.step && rule_type == "endpoint" {
        push_error(
            errors,
            "EndpointRuleNotAllowed",
            path,
            "endpoint rule not allowed as step",
            Some("type".to_string()),
            None,
        );
    }
    if usage.body_rule && rule_type != "normal" {
        push_error(
            errors,
            "BodyRuleInvalid",
            path,
            "body_rule must be normal",
            Some("type".to_string()),
            None,
        );
    }
    if usage.catch_rule && rule_type != "normal" {
        push_error(
            errors,
            "CatchRuleInvalid",
            path,
            "catch rule must be normal",
            Some("type".to_string()),
            None,
        );
    }
    if usage.branch_rule && rule_type != "normal" {
        push_error(
            errors,
            "BranchRuleInvalid",
            path,
            "branch rule must be normal",
            Some("type".to_string()),
            None,
        );
    }

    if !state.validated_content.insert(path.to_path_buf()) {
        return;
    }

    match rule_type.as_str() {
        "network" => validate_network_rule(&source, path, state, errors),
        "endpoint" => {}
        _ => validate_normal_rule(&source, path, state, errors),
    }
}

fn validate_normal_rule(
    source: &str,
    path: &Path,
    state: &mut ValidationState,
    errors: &mut Vec<RulesDirError>,
) {
    let rule = match parse_rule_file_with_format(source, RuleFormat::from_path(path)) {
        Ok(rule) => rule,
        Err(err) => {
            push_parse_error(errors, path, &err.to_string(), err.line_column());
            return;
        }
    };
    if let Err(rule_errors) = validate_rule_file_with_source(&rule, source) {
        for err in rule_errors {
            push_rule_error(errors, path, &err);
        }
    }
    if let Some(steps) = &rule.steps {
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        for step in steps {
            if let Some(branch) = &step.branch {
                if !branch.then.trim().is_empty() {
                    let resolved = resolve_rule_path(base_dir, branch.then.as_str());
                    validate_rule_path(&resolved, RuleRefUsage::branch_rule(), state, errors);
                }
                if let Some(r#else) = &branch.r#else {
                    if !r#else.trim().is_empty() {
                        let resolved = resolve_rule_path(base_dir, r#else.as_str());
                        validate_rule_path(&resolved, RuleRefUsage::branch_rule(), state, errors);
                    }
                }
            }
        }
    }
}

fn validate_network_rule(
    source: &str,
    path: &Path,
    state: &mut ValidationState,
    errors: &mut Vec<RulesDirError>,
) {
    let raw: NetworkRuleFile = match parse_yaml(path, source, errors) {
        Some(raw) => raw,
        None => return,
    };

    if raw.version != 2 {
        push_error(
            errors,
            "InvalidVersion",
            path,
            "network rule version must be 2",
            Some("version".to_string()),
            None,
        );
    }
    if raw.rule_type != "network" {
        push_error(
            errors,
            "InvalidRuleType",
            path,
            "network rule type must be network",
            Some("type".to_string()),
            None,
        );
    }
    if raw.body.is_some() && raw.body_map.is_some() {
        push_error(
            errors,
            "NetworkInvalidConfig",
            path,
            "body and body_map are mutually exclusive",
            Some("body".to_string()),
            None,
        );
    }
    if raw.body.is_some() && raw.body_rule.is_some() {
        push_error(
            errors,
            "NetworkInvalidConfig",
            path,
            "body and body_rule are mutually exclusive",
            Some("body".to_string()),
            None,
        );
    }
    if raw.body_map.is_some() && raw.body_rule.is_some() {
        push_error(
            errors,
            "NetworkInvalidConfig",
            path,
            "body_map and body_rule are mutually exclusive",
            Some("body_map".to_string()),
            None,
        );
    }

    let method = match Method::from_bytes(raw.request.method.as_bytes()) {
        Ok(method) => Some(method),
        Err(_) => {
            push_error(
                errors,
                "InvalidMethod",
                path,
                "invalid method",
                Some("request.method".to_string()),
                None,
            );
            None
        }
    };

    if let Some(method) = method {
        if method == Method::GET
            && (raw.body.is_some() || raw.body_map.is_some() || raw.body_rule.is_some())
        {
            push_error(
                errors,
                "NetworkInvalidConfig",
                path,
                "GET with body is not allowed",
                Some("request.method".to_string()),
                None,
            );
        }
    }

    if let Err(err) = parse_v2_expr(&raw.request.url) {
        push_error(
            errors,
            "InvalidExpr",
            path,
            format!("request.url: {}", err),
            Some("request.url".to_string()),
            None,
        );
    }
    if let Some(body) = &raw.body {
        if let Err(err) = parse_v2_expr(body) {
            push_error(
                errors,
                "InvalidExpr",
                path,
                format!("body: {}", err),
                Some("body".to_string()),
                None,
            );
        }
    }
    if let Some(headers) = &raw.request.headers {
        for (key, value) in headers {
            if let Err(err) = parse_v2_expr(value) {
                let field = format!("request.headers.{}", key);
                push_error(
                    errors,
                    "InvalidExpr",
                    path,
                    format!("{}: {}", field, err),
                    Some(field),
                    None,
                );
            }
        }
    }

    match parse_duration(&raw.timeout) {
        Ok(timeout) => {
            if timeout.is_zero() {
                push_error(
                    errors,
                    "InvalidTimeout",
                    path,
                    "timeout must be > 0",
                    Some("timeout".to_string()),
                    None,
                );
            }
        }
        Err(err) => {
            push_error(
                errors,
                "InvalidTimeout",
                path,
                err.to_string(),
                Some("timeout".to_string()),
                None,
            );
        }
    }

    if let Err(err) = compile_retry(raw.retry.as_ref()) {
        push_error(
            errors,
            "InvalidRetry",
            path,
            err.to_string(),
            Some("retry".to_string()),
            None,
        );
    }

    let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
    if let Some(body_rule) = raw.body_rule.as_deref() {
        let resolved = resolve_rule_path(base_dir, body_rule);
        validate_rule_path(&resolved, RuleRefUsage::body_rule(), state, errors);
    }
    if let Some(catch) = &raw.catch {
        for target in catch.values() {
            let resolved = resolve_rule_path(base_dir, target);
            validate_rule_path(&resolved, RuleRefUsage::catch_rule(), state, errors);
        }
    }
}
