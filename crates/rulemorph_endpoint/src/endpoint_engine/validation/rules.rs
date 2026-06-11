use std::path::Path;

use rulemorph::{
    LEGACY_V1_RULE_DEPRECATION_MESSAGE, RuleFormat, is_legacy_v1_rule, parse_rule_file_with_format,
    validate_rule_file_with_source,
};

use crate::endpoint_engine::resolve_rule_path;

use super::diagnostics::{RulesDirError, push_error, push_parse_error, push_rule_error};
use super::network::validate_network_rule;
use super::source::{parse_rule_type, read_rule_source};
use super::state::{RuleRefUsage, ValidationState};

pub(super) fn validate_rule_path(
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
    if is_legacy_v1_rule(&rule) {
        tracing::warn!(
            rule_path = %path.display(),
            message = LEGACY_V1_RULE_DEPRECATION_MESSAGE,
            "deprecated Rulemorph rule version"
        );
    }
    if let Some(steps) = &rule.steps {
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        for step in steps {
            if let Some(branch) = &step.branch {
                if !branch.then.trim().is_empty() {
                    let resolved = resolve_rule_path(base_dir, branch.then.as_str());
                    validate_rule_path(&resolved, RuleRefUsage::branch_rule(), state, errors);
                }
                if let Some(r#else) = &branch.r#else
                    && !r#else.trim().is_empty()
                {
                    let resolved = resolve_rule_path(base_dir, r#else.as_str());
                    validate_rule_path(&resolved, RuleRefUsage::branch_rule(), state, errors);
                }
            }
        }
    }
}
