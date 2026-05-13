use std::collections::HashSet;

use crate::error::{ErrorCode, RuleError, ValidationResult};
use crate::locator::YamlLocator;
use crate::model::{Mapping, RuleFile};
use crate::path::{PathToken, parse_path};
use crate::v2_parser::is_v2_expr;
use crate::v2_validator::{V2Scope, V2ValidationCtx, validate_no_cyclic_dependencies};

mod bool_expr;
mod expr;
mod expr_args;
mod input;
mod op_inventory;
mod refs;
mod scope;
mod v2_expr;

use self::bool_expr::validate_when_expr;
use self::expr::validate_expr;
use self::input::validate_input;
use self::refs::validate_source;
use self::scope::LocalScope;
use self::v2_expr::{
    expr_to_json_value, validate_finalize_wrap_value, validate_v2_condition_expr,
    validate_v2_condition_expr_with_scope, validate_v2_mapping_expr,
};

pub fn validate_rule_file(rule: &RuleFile) -> ValidationResult {
    validate_rule_file_with_locator(rule, None)
}

pub fn validate_rule_file_with_source(rule: &RuleFile, source: &str) -> ValidationResult {
    let locator = YamlLocator::from_str(source);
    validate_rule_file_with_locator(rule, Some(&locator))
}

fn validate_rule_file_with_locator(
    rule: &RuleFile,
    locator: Option<&YamlLocator>,
) -> ValidationResult {
    let mut ctx = ValidationCtx::new(locator);

    validate_version(rule, &mut ctx);
    validate_input(rule, &mut ctx);
    validate_steps(rule, &mut ctx);
    validate_record_when(rule, &mut ctx);
    validate_mappings(rule, &mut ctx);
    validate_finalize(rule, &mut ctx);

    ctx.finish()
}

fn validate_steps(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    let steps = match rule.steps.as_ref() {
        Some(steps) => steps,
        None => {
            if rule.mappings.is_empty() {
                ctx.push(
                    ErrorCode::MissingMappings,
                    "mappings is required when steps is not set",
                    "mappings",
                );
            }
            return;
        }
    };

    if rule.version != 2 {
        ctx.push(
            ErrorCode::InvalidStep,
            "steps is only supported in version 2",
            "steps",
        );
    }

    if !rule.mappings.is_empty() || rule.record_when.is_some() {
        ctx.push(
            ErrorCode::StepsMappingExclusive,
            "steps cannot be combined with mappings or record_when",
            "steps",
        );
    }

    let mut produced_targets: HashSet<Vec<PathToken>> = HashSet::new();
    let mut v2_targets_with_deps: Vec<(String, HashSet<String>)> = Vec::new();

    for (index, step) in steps.iter().enumerate() {
        let base = format!("steps[{}]", index);
        let step_kind_count = [
            step.mappings.is_some(),
            step.record_when.is_some(),
            step.asserts.is_some(),
            step.branch.is_some(),
        ]
        .into_iter()
        .filter(|v| *v)
        .count();

        if step_kind_count != 1 {
            ctx.push(
                ErrorCode::InvalidStep,
                "step must contain exactly one of mappings/record_when/asserts/branch",
                &base,
            );
            continue;
        }

        if let Some(mappings) = &step.mappings {
            validate_mappings_list(
                mappings,
                &format!("{}.mappings", base),
                &mut produced_targets,
                &mut v2_targets_with_deps,
                ctx,
                rule.version,
            );
        }

        if let Some(expr) = &step.record_when {
            let expr_path = format!("{}.record_when", base);
            if rule.version == 2 {
                if let Some(raw_value) = expr_to_json_value(expr) {
                    validate_v2_condition_expr(&raw_value, &expr_path, &produced_targets, ctx);
                    continue;
                }
            }
            validate_expr(expr, &expr_path, &produced_targets, ctx, LocalScope::None);
            validate_when_expr(expr, &expr_path, ctx);
        }

        if let Some(asserts) = &step.asserts {
            for (assert_idx, assert) in asserts.iter().enumerate() {
                let assert_path = format!("{}.asserts[{}]", base, assert_idx);
                if assert.error.code.trim().is_empty() || assert.error.message.trim().is_empty() {
                    ctx.push(
                        ErrorCode::InvalidStep,
                        "asserts.error.code and message are required",
                        &format!("{}.error", assert_path),
                    );
                }

                if rule.version == 2 {
                    if let Some(raw_value) = expr_to_json_value(&assert.when) {
                        validate_v2_condition_expr(
                            &raw_value,
                            &format!("{}.when", assert_path),
                            &produced_targets,
                            ctx,
                        );
                        continue;
                    }
                }
                validate_expr(
                    &assert.when,
                    &format!("{}.when", assert_path),
                    &produced_targets,
                    ctx,
                    LocalScope::None,
                );
                validate_when_expr(&assert.when, &format!("{}.when", assert_path), ctx);
            }
        }

        if let Some(branch) = &step.branch {
            let branch_path = format!("{}.branch", base);
            let when_path = format!("{}.when", branch_path);
            let mut v2_handled = false;
            if rule.version == 2 {
                if let Some(raw_value) = expr_to_json_value(&branch.when) {
                    validate_v2_condition_expr(&raw_value, &when_path, &produced_targets, ctx);
                    v2_handled = true;
                }
            }
            if !v2_handled {
                validate_expr(
                    &branch.when,
                    &when_path,
                    &produced_targets,
                    ctx,
                    LocalScope::None,
                );
                validate_when_expr(&branch.when, &when_path, ctx);
            }

            if branch.then.trim().is_empty() {
                ctx.push(
                    ErrorCode::InvalidStep,
                    "branch.then is required",
                    &format!("{}.then", branch_path),
                );
            }
            if let Some(r#else) = &branch.r#else {
                if r#else.trim().is_empty() {
                    ctx.push(
                        ErrorCode::InvalidStep,
                        "branch.else must not be empty",
                        &format!("{}.else", branch_path),
                    );
                }
            }
            if !branch.return_ {
                ctx.allow_any_out_ref = true;
            }
        }
    }

    if !v2_targets_with_deps.is_empty() {
        let mut v2_ctx = V2ValidationCtx::new(ctx.locator);
        validate_no_cyclic_dependencies(&v2_targets_with_deps, "steps", &mut v2_ctx);
        for err in v2_ctx.errors() {
            ctx.errors.push(err.clone());
        }
    }
}

fn validate_finalize(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    let finalize = match rule.finalize.as_ref() {
        Some(finalize) => finalize,
        None => return,
    };

    if rule.version != 2 {
        ctx.push(
            ErrorCode::InvalidFinalize,
            "finalize is only supported in version 2",
            "finalize",
        );
        return;
    }

    if let Some(filter) = &finalize.filter {
        let base_path = "finalize.filter";
        if let Some(raw_value) = expr_to_json_value(filter) {
            validate_v2_condition_expr_with_scope(
                &raw_value,
                base_path,
                &HashSet::new(),
                ctx,
                V2Scope::new().with_item(),
            );
        } else {
            ctx.push(
                ErrorCode::InvalidFinalize,
                "finalize.filter must be a v2 condition",
                base_path,
            );
        }
    }

    if let Some(sort) = &finalize.sort {
        let base_path = "finalize.sort";
        if parse_path(&sort.by).is_err() {
            ctx.push(
                ErrorCode::InvalidPath,
                "finalize.sort.by is invalid",
                format!("{}.by", base_path),
            );
        }
        if sort.order != "asc" && sort.order != "desc" {
            ctx.push(
                ErrorCode::InvalidFinalize,
                "finalize.sort.order must be asc or desc",
                format!("{}.order", base_path),
            );
        }
    }

    if let Some(wrap) = &finalize.wrap {
        let mut v2_ctx = V2ValidationCtx::with_produced_targets(ctx.locator, HashSet::new(), true);
        validate_finalize_wrap_value(wrap, "finalize.wrap", &mut v2_ctx);
        for err in v2_ctx.errors() {
            ctx.errors.push(err.clone());
        }
    }
}

fn validate_mappings_list(
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

fn validate_version(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    if rule.version != 1 && rule.version != 2 {
        ctx.push(
            ErrorCode::InvalidVersion,
            "version must be 1 or 2",
            "version",
        );
    }
}

fn validate_record_when(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
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

fn validate_mappings(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
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

struct ValidationCtx<'a> {
    locator: Option<&'a YamlLocator>,
    errors: Vec<RuleError>,
    allow_any_out_ref: bool,
}

impl<'a> ValidationCtx<'a> {
    fn new(locator: Option<&'a YamlLocator>) -> Self {
        Self {
            locator,
            errors: Vec::new(),
            allow_any_out_ref: false,
        }
    }

    fn push(&mut self, code: ErrorCode, message: &str, path: impl Into<String>) {
        let path = path.into();
        let mut err = RuleError::new(code, message).with_path(path.clone());
        if let Some(locator) = self.locator {
            if let Some(location) = locator.location_for(&path) {
                err = err.with_location(location.line, location.column);
            }
        }
        self.errors.push(err);
    }

    fn finish(self) -> ValidationResult {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors)
        }
    }
}
