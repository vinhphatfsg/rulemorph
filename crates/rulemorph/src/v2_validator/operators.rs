use crate::error::ErrorCode;
use crate::v2_model::V2OpStep;
use crate::v2_operator::{
    V2OperatorArgScope, is_valid_operator, operator_arg_range, operator_arg_scope,
};

use super::{V2Scope, V2ValidationCtx, validate_v2_expr};

/// Validate a v2 op step
pub(super) fn validate_v2_op_step(
    op_step: &V2OpStep,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    if ctx.is_custom_op(&op_step.op) {
        if !scope.allows_pipe() {
            ctx.push_error(
                ErrorCode::InvalidRefNamespace,
                "custom op direct call requires a pipe value; use with call options for literal-start calls",
                base_path,
            );
        }
        if !op_step.args.is_empty() {
            ctx.push_error(
                ErrorCode::InvalidArgs,
                "custom op arguments must use with call options",
                base_path,
            );
        }
        return;
    }

    if op_step.op == "object" {
        ctx.push_error(
            ErrorCode::InvalidExprShape,
            "object must use the object step form",
            base_path,
        );
        return;
    }

    // Check if op is known
    if !is_valid_op(&op_step.op) {
        ctx.push_error(
            ErrorCode::UnknownOp,
            format!("unknown operation: {}", op_step.op),
            base_path,
        );
    }

    // Validate argument count
    validate_op_args_count(&op_step.op, op_step.args.len(), base_path, ctx);

    // Validate each argument expression
    for (i, arg) in op_step.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, i);
        let arg_scope = get_arg_scope_for_op(&op_step.op, i, op_step.args.len(), scope);
        validate_v2_expr(arg, &arg_path, &arg_scope, ctx);
    }
}

pub(crate) fn is_valid_op(op: &str) -> bool {
    is_valid_operator(op)
}

/// Get the appropriate scope for an operation argument
fn get_arg_scope_for_op(
    op: &str,
    arg_index: usize,
    arg_count: usize,
    parent_scope: &V2Scope,
) -> V2Scope {
    match operator_arg_scope(op, arg_index, arg_count) {
        Some(V2OperatorArgScope::Item) => V2Scope::with_parent(parent_scope).with_item(),
        Some(V2OperatorArgScope::ItemAndAcc) => {
            V2Scope::with_parent(parent_scope).with_item().with_acc()
        }
        _ => parent_scope.clone(),
    }
}

/// Validate operation argument count
fn validate_op_args_count(op: &str, count: usize, base_path: &str, ctx: &mut V2ValidationCtx<'_>) {
    let (min, max) = get_op_arg_range(op);

    if count < min {
        ctx.push_error(
            ErrorCode::InvalidArgs,
            format!(
                "{} requires at least {} argument(s), got {}",
                op, min, count
            ),
            base_path,
        );
    } else if let Some(max_val) = max {
        if count > max_val {
            ctx.push_error(
                ErrorCode::InvalidArgs,
                format!(
                    "{} accepts at most {} argument(s), got {}",
                    op, max_val, count
                ),
                base_path,
            );
        }
    }
}

/// Get the valid argument count range for an operation
/// Returns (min, max) where max is None for unlimited
pub(super) fn get_op_arg_range(op: &str) -> (usize, Option<usize>) {
    operator_arg_range(op)
}
