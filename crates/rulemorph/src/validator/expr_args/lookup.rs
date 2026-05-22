use crate::error::ErrorCode;
use crate::model::{Expr, ExprOp};
use crate::path::parse_path;

use super::super::ValidationCtx;

pub(in crate::validator) fn validate_lookup_args_chain(
    expr_op: &ExprOp,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let len = expr_op.args.len();
    if !(2..=3).contains(&len) {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup args must be [key_path, match_value, output_path?] in chain",
            format!("{}.args", base_path),
        );
        return;
    }

    let key_path = literal_string(&expr_op.args[0]);
    if key_path.is_none() || key_path == Some("") {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup key_path must be a non-empty string literal",
            format!("{}.args[0]", base_path),
        );
    } else if parse_path(key_path.unwrap()).is_err() {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup key_path is invalid",
            format!("{}.args[0]", base_path),
        );
    }

    if len == 3 {
        let output_path = literal_string(&expr_op.args[2]);
        if output_path.is_none() || output_path == Some("") {
            ctx.push(
                ErrorCode::InvalidArgs,
                "lookup output_path must be a non-empty string literal",
                format!("{}.args[2]", base_path),
            );
        } else if parse_path(output_path.unwrap()).is_err() {
            ctx.push(
                ErrorCode::InvalidArgs,
                "lookup output_path is invalid",
                format!("{}.args[2]", base_path),
            );
        }
    }
}

pub(in crate::validator) fn validate_lookup_args(
    expr_op: &ExprOp,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let len = expr_op.args.len();
    if !(3..=4).contains(&len) {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup args must be [collection, key_path, match_value, output_path?]",
            format!("{}.args", base_path),
        );
        return;
    }

    let key_path = literal_string(&expr_op.args[1]);
    if key_path.is_none() || key_path == Some("") {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup key_path must be a non-empty string literal",
            format!("{}.args[1]", base_path),
        );
    } else if parse_path(key_path.unwrap()).is_err() {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup key_path is invalid",
            format!("{}.args[1]", base_path),
        );
    }

    if len == 4 {
        let output_path = literal_string(&expr_op.args[3]);
        if output_path.is_none() || output_path == Some("") {
            ctx.push(
                ErrorCode::InvalidArgs,
                "lookup output_path must be a non-empty string literal",
                format!("{}.args[3]", base_path),
            );
        } else if parse_path(output_path.unwrap()).is_err() {
            ctx.push(
                ErrorCode::InvalidArgs,
                "lookup output_path is invalid",
                format!("{}.args[3]", base_path),
            );
        }
    }
}

fn literal_string(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(value) => value.as_str(),
        _ => None,
    }
}
