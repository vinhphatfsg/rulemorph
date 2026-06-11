use std::path::Path;

use axum::http::Method;
use rulemorph::v2_parser::parse_v2_expr;

use super::diagnostics::{RulesDirError, push_error};
use super::source::parse_yaml;
use super::state::{RuleRefUsage, ValidationState};
use super::validate_rule_path;
use crate::endpoint_engine::{NetworkRuleFile, compile_retry, parse_duration, resolve_rule_path};

pub(super) fn validate_network_rule(
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

    if let Some(method) = method
        && method == Method::GET
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
    if let Some(body) = &raw.body
        && let Err(err) = parse_v2_expr(body)
    {
        push_error(
            errors,
            "InvalidExpr",
            path,
            format!("body: {}", err),
            Some("body".to_string()),
            None,
        );
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
