use std::collections::HashSet;

use serde_json::Value as JsonValue;

use crate::error::ErrorCode;
use crate::model::Expr;
use crate::path::{PathToken, parse_path};
use crate::v2_parser::{
    is_literal_escape, is_pipe_value, is_v2_ref, parse_v2_condition, parse_v2_expr,
};
use crate::v2_validator::{
    V2Scope, V2ValidationCtx, collect_out_references, validate_v2_condition, validate_v2_expr,
};

use super::ValidationCtx;
use super::refs::out_ref_resolves_in_targets;

/// Convert Expr to JsonValue for v2 validation
/// Also handles the case where a single-element v2 pipe array gets deserialized as ExprRef
pub(super) fn expr_to_json_value(expr: &Expr) -> Option<serde_json::Value> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        // Handle serde_yaml quirk: single-element YAML array ["@ref"] or ["lit:..."]
        // gets deserialized as ExprRef, but should be treated as v2 expr.
        Expr::Ref(ref_expr)
            if is_v2_ref(&ref_expr.ref_path)
                || is_pipe_value(&ref_expr.ref_path)
                || is_literal_escape(&ref_expr.ref_path) =>
        {
            // Convert back to a single-element array for v2 parsing
            Some(serde_json::Value::Array(vec![serde_json::Value::String(
                ref_expr.ref_path.clone(),
            )]))
        }
        Expr::Chain(chain) => {
            if let Some(first) = chain.chain.first()
                && expr_starts_v2_pipe(first)
            {
                let arr: Vec<JsonValue> = chain.chain.iter().map(expr_to_json_literal).collect();
                return Some(JsonValue::Array(arr));
            }
            None
        }
        // For v1 expressions (Ref, Op, Chain), return None
        // These will be handled by v1 validator
        _ => None,
    }
}

fn expr_to_json_literal(expr: &Expr) -> JsonValue {
    match expr {
        Expr::Ref(reference) => JsonValue::String(reference.ref_path.clone()),
        Expr::Literal(value) => value.clone(),
        Expr::Op(op) => {
            let mut obj = serde_json::Map::new();
            let args: Vec<JsonValue> = op.args.iter().map(expr_to_json_literal).collect();
            obj.insert(op.op.clone(), JsonValue::Array(args));
            JsonValue::Object(obj)
        }
        Expr::Chain(chain) => {
            JsonValue::Array(chain.chain.iter().map(expr_to_json_literal).collect())
        }
    }
}

fn expr_starts_v2_pipe(expr: &Expr) -> bool {
    match expr {
        Expr::Ref(reference) => {
            is_v2_ref(&reference.ref_path)
                || is_pipe_value(&reference.ref_path)
                || is_literal_escape(&reference.ref_path)
        }
        Expr::Literal(JsonValue::String(value)) => {
            is_v2_ref(value) || is_pipe_value(value) || is_literal_escape(value)
        }
        _ => false,
    }
}

/// Validate a v2 mapping expression
pub(super) fn validate_v2_mapping_expr(
    raw_value: &serde_json::Value,
    expr_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    target: &str,
    ctx: &mut ValidationCtx<'_>,
    v2_targets_with_deps: &mut Vec<(String, HashSet<String>)>,
) {
    // Parse v2 expression
    let v2_expr = match parse_v2_expr(raw_value) {
        Ok(expr) => expr,
        Err(e) => {
            ctx.push(
                ErrorCode::InvalidExprShape,
                &format!("invalid v2 expression: {:?}", e),
                expr_path,
            );
            return;
        }
    };

    // Create v2 validation context with parent outputs plus branch-child
    // outputs that are visible through @out after return:false branches.
    let ref_targets = out_ref_targets(produced_targets, ctx);
    let mut v2_ctx =
        V2ValidationCtx::with_produced_targets(ctx.locator, ref_targets, ctx.allow_any_out_ref)
            .with_custom_op_names(ctx.custom_op_names.clone());
    let scope = V2Scope::new();

    // Validate the v2 expression
    validate_v2_expr(&v2_expr, expr_path, &scope, &mut v2_ctx);

    // When branch(return=false) is present, @out can be a forward ref, so the
    // dependency graph is not reliable for cycle detection.
    if !ctx.allow_any_out_ref {
        let deps: HashSet<String> = collect_out_references(&v2_expr)
            .into_iter()
            .filter(|dep| !out_dep_resolves_only_in_branch_outputs(dep, produced_targets, ctx))
            .collect();
        if !deps.is_empty() {
            v2_targets_with_deps.push((target.to_string(), deps));
        }
    }

    // Transfer errors from v2 context to main context
    for err in v2_ctx.errors() {
        ctx.errors.push(err.clone());
    }
}

pub(super) fn validate_v2_condition_expr(
    raw_value: &serde_json::Value,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
) {
    validate_v2_condition_expr_with_scope(
        raw_value,
        base_path,
        produced_targets,
        ctx,
        V2Scope::new(),
    );
}

pub(super) fn validate_v2_condition_expr_with_scope(
    raw_value: &serde_json::Value,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
    scope: V2Scope,
) {
    let condition = match parse_v2_condition(raw_value) {
        Ok(cond) => cond,
        Err(e) => {
            ctx.push(
                ErrorCode::InvalidExprShape,
                &format!("invalid v2 condition: {:?}", e),
                base_path,
            );
            return;
        }
    };

    let mut v2_ctx = V2ValidationCtx::with_produced_targets(
        ctx.locator,
        out_ref_targets(produced_targets, ctx),
        ctx.allow_any_out_ref,
    )
    .with_custom_op_names(ctx.custom_op_names.clone());
    validate_v2_condition(&condition, base_path, &scope, &mut v2_ctx);

    for err in v2_ctx.errors() {
        ctx.errors.push(err.clone());
    }
}

fn out_ref_targets(
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &ValidationCtx<'_>,
) -> HashSet<Vec<PathToken>> {
    let mut targets = produced_targets.clone();
    targets.extend(ctx.branch_out_ref_targets.iter().cloned());
    targets
}

fn out_dep_resolves_only_in_branch_outputs(
    dep: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &ValidationCtx<'_>,
) -> bool {
    let Ok(tokens) = parse_path(dep) else {
        return false;
    };
    !out_ref_resolves_in_targets(&tokens, produced_targets)
        && out_ref_resolves_in_targets(&tokens, &ctx.branch_out_ref_targets)
}

pub(super) fn validate_finalize_wrap_value(
    value: &JsonValue,
    base_path: &str,
    v2_ctx: &mut V2ValidationCtx<'_>,
) {
    match value {
        JsonValue::Object(map) => {
            for (key, value) in map {
                let child_path = format!("{}.{}", base_path, key);
                validate_finalize_wrap_value(value, &child_path, v2_ctx);
            }
        }
        _ => {
            let v2_expr = match parse_v2_expr(value) {
                Ok(expr) => expr,
                Err(e) => {
                    v2_ctx.push_error(
                        ErrorCode::InvalidExprShape,
                        format!("invalid v2 expression: {:?}", e),
                        base_path,
                    );
                    return;
                }
            };
            let scope = V2Scope::new();
            validate_v2_expr(&v2_expr, base_path, &scope, v2_ctx);
        }
    }
}
