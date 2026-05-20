use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use rulemorph::{RuleFormat, parse_rule_file_with_format, validate_rule_file_with_source};

use super::{CompiledEndpointRule, EndpointRuleFile, resolve_rule_path};

mod diagnostics;
mod network;
mod source;
mod state;

pub use self::diagnostics::{RulesDirError, RulesDirErrors};
use self::diagnostics::{push_error, push_parse_error, push_rule_error};
use self::network::validate_network_rule;
use self::source::{parse_rule_type, parse_yaml, read_rule_source};
use self::state::{RuleRefUsage, ValidationState};

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
