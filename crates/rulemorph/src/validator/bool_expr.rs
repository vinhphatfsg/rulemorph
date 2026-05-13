use crate::error::ErrorCode;
use crate::model::{Expr, ExprChain, ExprOp};

use super::ValidationCtx;

#[derive(Clone, Copy, PartialEq, Eq)]
enum BoolExprKind {
    Bool,
    Maybe,
    NotBool,
}

pub(super) fn validate_when_expr(expr: &Expr, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    if matches!(bool_expr_kind(expr), BoolExprKind::NotBool) {
        ctx.push(
            ErrorCode::InvalidWhenType,
            "when/record_when must evaluate to boolean",
            base_path,
        );
    }
}

fn bool_expr_kind(expr: &Expr) -> BoolExprKind {
    match expr {
        Expr::Literal(value) => {
            if value.is_boolean() {
                BoolExprKind::Bool
            } else {
                BoolExprKind::NotBool
            }
        }
        Expr::Ref(_) => BoolExprKind::Maybe,
        Expr::Op(expr_op) => match expr_op.op.as_str() {
            "concat" | "to_string" | "trim" | "lowercase" | "uppercase" | "replace" | "split"
            | "pad_start" | "pad_end" | "lookup" | "lookup_first" | "merge" | "deep_merge"
            | "get" | "pick" | "omit" | "keys" | "values" | "entries" | "len" | "from_entries"
            | "object_flatten" | "object_unflatten" | "map" | "filter" | "flat_map" | "flatten"
            | "take" | "drop" | "slice" | "chunk" | "zip" | "zip_with" | "unzip" | "group_by"
            | "key_by" | "partition" | "unique" | "distinct_by" | "sort_by" | "find_index"
            | "index_of" | "sum" | "avg" | "min" | "max" | "+" | "-" | "*" | "/" | "round"
            | "to_base" | "date_format" | "to_unixtime" => BoolExprKind::NotBool,
            "and" | "or" | "not" | "contains" => BoolExprKind::Bool,
            "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => BoolExprKind::Bool,
            "coalesce" => {
                let mut saw_maybe = false;
                for arg in &expr_op.args {
                    match bool_expr_kind(arg) {
                        BoolExprKind::Bool => {}
                        BoolExprKind::Maybe => saw_maybe = true,
                        BoolExprKind::NotBool => return BoolExprKind::NotBool,
                    }
                }
                if saw_maybe {
                    BoolExprKind::Maybe
                } else {
                    BoolExprKind::Bool
                }
            }
            _ => BoolExprKind::Maybe,
        },
        Expr::Chain(expr_chain) => bool_expr_kind_chain(expr_chain),
    }
}

fn bool_expr_kind_chain(expr_chain: &ExprChain) -> BoolExprKind {
    if expr_chain.chain.is_empty() {
        return BoolExprKind::NotBool;
    }

    let mut current = bool_expr_kind(&expr_chain.chain[0]);
    for step in expr_chain.chain.iter().skip(1) {
        let expr_op = match step {
            Expr::Op(expr_op) => expr_op,
            _ => return BoolExprKind::Maybe,
        };
        current = bool_expr_kind_for_op_with_input(expr_op, current);
    }
    current
}

fn bool_expr_kind_for_op_with_input(expr_op: &ExprOp, injected: BoolExprKind) -> BoolExprKind {
    match expr_op.op.as_str() {
        "concat" | "to_string" | "trim" | "lowercase" | "uppercase" | "replace" | "split"
        | "pad_start" | "pad_end" | "lookup" | "lookup_first" | "merge" | "deep_merge" | "get"
        | "pick" | "omit" | "keys" | "values" | "entries" | "len" | "from_entries"
        | "object_flatten" | "object_unflatten" | "map" | "filter" | "flat_map" | "flatten"
        | "take" | "drop" | "slice" | "chunk" | "zip" | "zip_with" | "unzip" | "group_by"
        | "key_by" | "partition" | "unique" | "distinct_by" | "sort_by" | "find_index"
        | "index_of" | "sum" | "avg" | "min" | "max" | "+" | "-" | "*" | "/" | "round"
        | "to_base" | "date_format" | "to_unixtime" => BoolExprKind::NotBool,
        "and" | "or" | "not" | "contains" => BoolExprKind::Bool,
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => BoolExprKind::Bool,
        "coalesce" => {
            let mut saw_maybe = matches!(injected, BoolExprKind::Maybe);
            if matches!(injected, BoolExprKind::NotBool) {
                return BoolExprKind::NotBool;
            }
            for arg in &expr_op.args {
                match bool_expr_kind(arg) {
                    BoolExprKind::Bool => {}
                    BoolExprKind::Maybe => saw_maybe = true,
                    BoolExprKind::NotBool => return BoolExprKind::NotBool,
                }
            }
            if saw_maybe {
                BoolExprKind::Maybe
            } else {
                BoolExprKind::Bool
            }
        }
        _ => BoolExprKind::Maybe,
    }
}
