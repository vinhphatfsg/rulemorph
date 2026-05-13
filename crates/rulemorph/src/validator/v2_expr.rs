use std::collections::HashSet;

use serde_json::Value as JsonValue;

use crate::error::ErrorCode;
use crate::model::Expr;
use crate::path::PathToken;
use crate::v2_parser::{is_literal_escape, parse_v2_condition, parse_v2_expr};
use crate::v2_validator::{
    V2Scope, V2ValidationCtx, collect_out_references, validate_v2_condition, validate_v2_expr,
};

use super::ValidationCtx;

/// Convert Expr to JsonValue for v2 validation
/// Also handles the case where a single-element v2 pipe array gets deserialized as ExprRef
pub(super) fn expr_to_json_value(expr: &Expr) -> Option<serde_json::Value> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        // Handle serde_yaml quirk: single-element YAML array ["@ref"] or ["lit:..."]
        // gets deserialized as ExprRef, but should be treated as v2 expr.
        Expr::Ref(ref_expr)
            if ref_expr.ref_path.starts_with('@') || is_literal_escape(&ref_expr.ref_path) =>
        {
            // Convert back to a single-element array for v2 parsing
            Some(serde_json::Value::Array(vec![serde_json::Value::String(
                ref_expr.ref_path.clone(),
            )]))
        }
        // For v1 expressions (Ref, Op, Chain), return None
        // These will be handled by v1 validator
        _ => None,
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

    // Create v2 validation context with produced targets
    let mut v2_ctx = V2ValidationCtx::with_produced_targets(
        ctx.locator,
        produced_targets.clone(),
        ctx.allow_any_out_ref,
    );
    let scope = V2Scope::new();

    // Validate the v2 expression
    validate_v2_expr(&v2_expr, expr_path, &scope, &mut v2_ctx);

    // When branch(return=false) is present, @out can be a forward ref, so the
    // dependency graph is not reliable for cycle detection.
    if !ctx.allow_any_out_ref {
        let deps = collect_out_references(&v2_expr);
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
        produced_targets.clone(),
        ctx.allow_any_out_ref,
    );
    validate_v2_condition(&condition, base_path, &scope, &mut v2_ctx);

    for err in v2_ctx.errors() {
        ctx.errors.push(err.clone());
    }
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
