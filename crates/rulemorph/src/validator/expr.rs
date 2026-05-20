use std::collections::HashSet;

use crate::error::ErrorCode;
use crate::model::{Expr, ExprOp};
use crate::path::PathToken;

use super::ValidationCtx;
use super::op_inventory::{element_expr_scope, is_valid_op};
use super::refs::validate_ref;
use super::scope::LocalScope;

mod chain;
mod op_args;

pub(super) fn validate_expr(
    expr: &Expr,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
    scope: LocalScope,
) {
    match expr {
        Expr::Ref(expr_ref) => validate_ref(expr_ref, base_path, produced_targets, ctx, scope),
        Expr::Op(expr_op) => validate_op(expr_op, base_path, produced_targets, ctx, scope),
        Expr::Chain(expr_chain) => {
            chain::validate_chain(expr_chain, base_path, produced_targets, ctx, scope)
        }
        Expr::Literal(_) => {}
    }
}

fn validate_op(
    expr_op: &ExprOp,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
    scope: LocalScope,
) {
    if !is_valid_op(&expr_op.op) {
        ctx.push(
            ErrorCode::UnknownOp,
            "expr.op is not supported",
            format!("{}.op", base_path),
        );
    }

    if expr_op.args.is_empty() {
        ctx.push(
            ErrorCode::InvalidArgs,
            "expr.args must be a non-empty array",
            format!("{}.args", base_path),
        );
    }

    op_args::validate_op_args(expr_op, base_path, ctx);

    let expr_scope = element_expr_scope(&expr_op.op, false, expr_op.args.len(), scope);
    for (index, arg) in expr_op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let arg_scope = match expr_scope {
            Some((expr_index, expr_scope)) if expr_index == index => expr_scope,
            _ => scope,
        };
        validate_expr(arg, &arg_path, produced_targets, ctx, arg_scope);
    }
}
