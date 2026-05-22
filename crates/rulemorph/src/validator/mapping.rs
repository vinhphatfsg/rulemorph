use std::collections::HashSet;

use crate::error::ErrorCode;
use crate::model::{Mapping, RuleFile};
use crate::path::{PathToken, parse_path};
use crate::v2_parser::is_v2_expr;
use crate::v2_validator::{V2ValidationCtx, validate_no_cyclic_dependencies};

use super::ValidationCtx;
use super::bool_expr::validate_when_expr;
use super::expr::validate_expr;
use super::refs::validate_source;
use super::scope::LocalScope;
use super::v2_expr::{expr_to_json_value, validate_v2_condition_expr, validate_v2_mapping_expr};

pub(super) fn validate_mappings_list(
    mappings: &[Mapping],
    base_path: &str,
    produced_targets: &mut HashSet<Vec<PathToken>>,
    v2_targets_with_deps: &mut Vec<(String, HashSet<String>)>,
    ctx: &mut ValidationCtx<'_>,
    rule_version: u8,
) {
    let is_v2_rule = rule_version == 2;
    for (index, mapping) in mappings.iter().enumerate() {
        let base = format!("{}[{}]", base_path, index);

        if mapping.target.trim().is_empty() {
            ctx.push(
                ErrorCode::MissingTarget,
                "mapping.target is required",
                format!("{}.target", base),
            );
        }

        let target_tokens = match parse_path(&mapping.target) {
            Ok(tokens) => tokens,
            Err(_) => {
                ctx.push(
                    ErrorCode::InvalidPath,
                    "target path is invalid",
                    format!("{}.target", base),
                );
                continue;
            }
        };
        if target_tokens
            .iter()
            .any(|token| matches!(token, PathToken::Index(_)))
        {
            ctx.push(
                ErrorCode::InvalidPath,
                "target path must not include indexes",
                format!("{}.target", base),
            );
            continue;
        }

        if produced_targets.contains(&target_tokens) {
            ctx.push(
                ErrorCode::DuplicateTarget,
                "mapping.target is duplicated",
                format!("{}.target", base),
            );
        }

        let value_count = count_value_fields(mapping);
        if value_count == 0 {
            ctx.push(
                ErrorCode::MissingMappingValue,
                "mapping must define source, value, or expr",
                base.clone(),
            );
        } else if value_count > 1 {
            ctx.push(
                ErrorCode::SourceValueExprExclusive,
                "exactly one of source/value/expr is required",
                base.clone(),
            );
        }

        if let Some(type_name) = &mapping.value_type {
            if !is_valid_type_name(type_name) {
                ctx.push(
                    ErrorCode::InvalidTypeName,
                    "type must be string|int|float|bool",
                    format!("{}.type", base),
                );
            }
        }

        if let Some(source) = &mapping.source {
            validate_source(source, &base, produced_targets, ctx);
        }

        if let Some(expr) = &mapping.expr {
            let expr_path = format!("{}.expr", base);
            let mut v2_handled = false;
            if is_v2_rule {
                if let Some(raw_value) = expr_to_json_value(expr) {
                    if is_v2_expr(&raw_value) {
                        validate_v2_mapping_expr(
                            &raw_value,
                            &expr_path,
                            produced_targets,
                            &mapping.target,
                            ctx,
                            v2_targets_with_deps,
                        );
                        v2_handled = true;
                    }
                }
            }
            if !v2_handled {
                validate_expr(expr, &expr_path, produced_targets, ctx, LocalScope::None);
            }
        }

        if let Some(when) = &mapping.when {
            let when_path = format!("{}.when", base);
            let mut v2_handled = false;
            if is_v2_rule {
                if let Some(raw_value) = expr_to_json_value(when) {
                    if is_v2_expr(&raw_value) {
                        validate_v2_condition_expr(&raw_value, &when_path, produced_targets, ctx);
                        v2_handled = true;
                    }
                }
            }
            if !v2_handled {
                validate_expr(when, &when_path, produced_targets, ctx, LocalScope::None);
                validate_when_expr(when, &when_path, ctx);
            }
        }

        produced_targets.insert(target_tokens);
    }
}

pub(super) fn validate_record_when(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    if rule.steps.is_some() {
        return;
    }
    let expr = match rule.record_when.as_ref() {
        Some(expr) => expr,
        None => return,
    };

    let base_path = "record_when";
    let produced_targets = HashSet::new();
    if rule.version == 2 {
        if let Some(raw_value) = expr_to_json_value(expr) {
            validate_v2_condition_expr(&raw_value, base_path, &produced_targets, ctx);
            return;
        }
    }

    validate_expr(expr, base_path, &produced_targets, ctx, LocalScope::None);
    validate_when_expr(expr, base_path, ctx);
}

pub(super) fn validate_mappings(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    if rule.steps.is_some() {
        return;
    }

    let mut produced_targets: HashSet<Vec<PathToken>> = HashSet::new();
    let mut v2_targets_with_deps: Vec<(String, HashSet<String>)> = Vec::new();

    validate_mappings_list(
        &rule.mappings,
        "mappings",
        &mut produced_targets,
        &mut v2_targets_with_deps,
        ctx,
        rule.version,
    );

    if rule.version == 2 && !v2_targets_with_deps.is_empty() {
        let mut v2_ctx = V2ValidationCtx::new(ctx.locator);
        validate_no_cyclic_dependencies(&v2_targets_with_deps, "mappings", &mut v2_ctx);
        for err in v2_ctx.errors() {
            ctx.errors.push(err.clone());
        }
    }
}

fn count_value_fields(mapping: &Mapping) -> usize {
    let mut count = 0;
    if mapping.source.is_some() {
        count += 1;
    }
    if mapping.value.is_some() {
        count += 1;
    }
    if mapping.expr.is_some() {
        count += 1;
    }
    count
}

fn is_valid_type_name(value: &str) -> bool {
    matches!(value, "string" | "int" | "float" | "bool")
}
