use std::collections::HashSet;

use crate::error::ErrorCode;
use crate::model::RuleFile;
use crate::path::PathToken;
use crate::v2_validator::{V2ValidationCtx, validate_no_cyclic_dependencies};

use super::ValidationCtx;
use super::bool_expr::validate_when_expr;
use super::branch_graph::collect_branch_contract;
use super::expr::validate_expr;
use super::mapping::validate_mappings_list;
use super::scope::LocalScope;
use super::v2_expr::{expr_to_json_value, validate_v2_condition_expr};

pub(super) fn validate_steps(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
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
            if rule.version == 2
                && let Some(raw_value) = expr_to_json_value(expr)
            {
                validate_v2_condition_expr(&raw_value, &expr_path, &produced_targets, ctx);
                continue;
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
                        format!("{}.error", assert_path),
                    );
                }

                if rule.version == 2
                    && let Some(raw_value) = expr_to_json_value(&assert.when)
                {
                    validate_v2_condition_expr(
                        &raw_value,
                        &format!("{}.when", assert_path),
                        &produced_targets,
                        ctx,
                    );
                    continue;
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
            if rule.version == 2
                && let Some(raw_value) = expr_to_json_value(&branch.when)
            {
                validate_v2_condition_expr(&raw_value, &when_path, &produced_targets, ctx);
                v2_handled = true;
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
                    format!("{}.then", branch_path),
                );
            }
            if let Some(r#else) = &branch.r#else
                && r#else.trim().is_empty()
            {
                ctx.push(
                    ErrorCode::InvalidStep,
                    "branch.else must not be empty",
                    format!("{}.else", branch_path),
                );
            }
            if let Some(contract) = collect_branch_contract(ctx, branch, &branch_path)
                && !branch.return_
            {
                ctx.branch_out_ref_targets.extend(contract.possible_outputs);
            } else if !branch.return_ && ctx.branch_graph.is_none() {
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
