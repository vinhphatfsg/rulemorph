use crate::error::ErrorCode;
use crate::model::ExprOp;

use super::super::super::ValidationCtx;
use super::super::super::expr_args::{
    validate_lookup_args_chain, validate_path_arg, validate_path_array_arg,
};

pub(super) fn validate_chain_op_args(
    expr_op: &ExprOp,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let args_len = expr_op.args.len() + 1;
    match expr_op.op.as_str() {
        "trim" | "lowercase" | "uppercase" | "to_string" | "len" | "not" => {
            if args_len != 1 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly one item",
                    format!("{}.args", base_path),
                );
            }
        }
        "replace" => {
            if !(3..=4).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain three or four items",
                    format!("{}.args", base_path),
                );
            }
        }
        "split" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "pad_start" | "pad_end" => {
            if !(2..=3).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two or three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "lookup" | "lookup_first" => {
            validate_lookup_args_chain(expr_op, base_path, ctx);
        }
        "merge" | "deep_merge" => {
            if args_len < 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "get" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            } else {
                validate_path_arg(&expr_op.args[0], &format!("{}.args[0]", base_path), ctx);
            }
        }
        "pick" | "omit" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            } else {
                let allow_terminal_index = expr_op.op == "pick";
                validate_path_array_arg(
                    &expr_op.args[0],
                    &format!("{}.args[0]", base_path),
                    allow_terminal_index,
                    ctx,
                );
            }
        }
        "keys" | "values" | "entries" | "object_flatten" | "object_unflatten" => {
            if args_len != 1 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly one item",
                    format!("{}.args", base_path),
                );
            }
        }
        "from_entries" => {
            if !(1..=2).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain one or two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "map" | "filter" | "flat_map" | "group_by" | "key_by" | "partition" | "distinct_by"
        | "find" | "find_index" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "flatten" => {
            if !(1..=2).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain one or two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "take" | "drop" | "chunk" | "index_of" | "contains" | "reduce" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "slice" => {
            if !(2..=3).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two or three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "zip" => {
            if args_len < 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "zip_with" => {
            if args_len < 3 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "unzip" | "unique" | "sum" | "avg" | "min" | "max" => {
            if args_len != 1 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly one item",
                    format!("{}.args", base_path),
                );
            }
        }
        "sort_by" => {
            if !(2..=3).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two or three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "fold" => {
            if args_len != 3 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "+" | "*" | "and" | "or" => {
            if args_len < 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "-" | "/" | "to_base" | "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "round" => {
            if !(1..=2).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain one or two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "date_format" => {
            if !(2..=4).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two to four items",
                    format!("{}.args", base_path),
                );
            }
        }
        "to_unixtime" => {
            if !(1..=3).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain one to three items",
                    format!("{}.args", base_path),
                );
            }
        }
        _ => {}
    }
}
