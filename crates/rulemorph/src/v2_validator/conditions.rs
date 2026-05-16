use crate::error::ErrorCode;
use crate::v2_model::{V2Comparison, V2Condition};

use super::{V2Scope, V2ValidationCtx, infer_v2_expr_type, validate_v2_expr};

/// Validate a v2 condition
pub fn validate_v2_condition(
    cond: &V2Condition,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match cond {
        V2Condition::All(conditions) => {
            for (i, c) in conditions.iter().enumerate() {
                let path = format!("{}.all[{}]", base_path, i);
                validate_v2_condition(c, &path, scope, ctx);
            }
        }
        V2Condition::Any(conditions) => {
            for (i, c) in conditions.iter().enumerate() {
                let path = format!("{}.any[{}]", base_path, i);
                validate_v2_condition(c, &path, scope, ctx);
            }
        }
        V2Condition::Comparison(comp) => {
            validate_v2_comparison(comp, base_path, scope, ctx);
        }
        V2Condition::Expr(expr) => {
            validate_v2_expr(expr, base_path, scope, ctx);
            // Type check: must be bool or unknown
            let typ = infer_v2_expr_type(expr);
            if typ.is_definitely_not_bool() {
                ctx.push_error(
                    ErrorCode::InvalidWhenType,
                    "condition must evaluate to boolean",
                    base_path,
                );
            }
        }
    }
}

/// Validate a v2 comparison
fn validate_v2_comparison(
    comp: &V2Comparison,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Comparisons need exactly 2 arguments
    if comp.args.len() != 2 {
        ctx.push_error(
            ErrorCode::InvalidArgs,
            format!(
                "comparison requires exactly 2 arguments, got {}",
                comp.args.len()
            ),
            base_path,
        );
    }

    // Validate each argument
    for (i, arg) in comp.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, i);
        validate_v2_expr(arg, &arg_path, scope, ctx);
    }
}
