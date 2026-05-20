use std::collections::HashSet;

use crate::error::ErrorCode;
use crate::model::{Expr, ExprChain, ExprOp};
use crate::path::PathToken;

mod op_args;

use self::op_args::validate_chain_op_args;
use super::super::ValidationCtx;
use super::super::op_inventory::{element_expr_scope, is_valid_op};
use super::super::scope::LocalScope;
use super::validate_expr;

pub(super) fn validate_chain(
    expr_chain: &ExprChain,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
    scope: LocalScope,
) {
    if expr_chain.chain.is_empty() {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "expr.chain must be a non-empty array",
            format!("{}.chain", base_path),
        );
        return;
    }

    for (index, item) in expr_chain.chain.iter().enumerate() {
        let item_path = format!("{}.chain[{}]", base_path, index);
        if index == 0 {
            validate_expr(item, &item_path, produced_targets, ctx, scope);
            continue;
        }

        match item {
            Expr::Op(expr_op) => {
                validate_chain_op(expr_op, &item_path, produced_targets, ctx, scope);
            }
            _ => {
                ctx.push(
                    ErrorCode::InvalidExprShape,
                    "expr.chain items after first must be op",
                    item_path,
                );
            }
        }
    }
}

fn validate_chain_op(
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

    validate_chain_op_args(expr_op, base_path, ctx);

    let expr_scope = element_expr_scope(&expr_op.op, true, expr_op.args.len(), scope);
    for (index, arg) in expr_op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let arg_scope = match expr_scope {
            Some((expr_index, expr_scope)) if expr_index == index => expr_scope,
            _ => scope,
        };
        validate_expr(arg, &arg_path, produced_targets, ctx, arg_scope);
    }
}
