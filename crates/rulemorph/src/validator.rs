use std::collections::HashSet;

use crate::error::{ErrorCode, RuleError, ValidationResult};
use crate::locator::YamlLocator;
use crate::model::RuleFile;
use crate::path::parse_path;
use crate::v2_validator::{V2Scope, V2ValidationCtx};

mod bool_expr;
mod expr;
mod expr_args;
mod input;
mod mapping;
mod op_inventory;
mod refs;
mod scope;
mod steps;
mod v2_expr;

use self::input::validate_input;
use self::mapping::{validate_mappings, validate_record_when};
use self::steps::validate_steps;
use self::v2_expr::{
    expr_to_json_value, validate_finalize_wrap_value, validate_v2_condition_expr_with_scope,
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

fn validate_version(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    if rule.version != 1 && rule.version != 2 {
        ctx.push(
            ErrorCode::InvalidVersion,
            "version must be 1 or 2",
            "version",
        );
    }
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
