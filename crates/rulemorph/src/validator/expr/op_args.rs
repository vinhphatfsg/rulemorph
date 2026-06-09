use crate::model::ExprOp;

use super::super::ValidationCtx;
use super::super::expr_args::{validate_lookup_args, validate_path_arg, validate_path_array_arg};
use super::arg_count::ArgCountRule::*;
use super::arg_count::require;

pub(super) fn validate_op_args(expr_op: &ExprOp, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    let args_len = expr_op.args.len();
    match expr_op.op.as_str() {
        "trim" | "lowercase" | "uppercase" | "to_string" | "len" => {
            require(args_len, ExactlyOne, base_path, ctx);
        }
        "replace" => {
            require(args_len, ThreeOrFour, base_path, ctx);
        }
        "split" => {
            require(args_len, ExactlyTwo, base_path, ctx);
        }
        "pad_start" | "pad_end" => {
            require(args_len, TwoOrThree, base_path, ctx);
        }
        "lookup" | "lookup_first" => {
            validate_lookup_args(expr_op, base_path, ctx);
        }
        "merge" | "deep_merge" => {
            require(args_len, AtLeastTwo, base_path, ctx);
        }
        "get" => {
            if args_len != 2 {
                require(args_len, ExactlyTwo, base_path, ctx);
            } else {
                validate_path_arg(&expr_op.args[1], &format!("{}.args[1]", base_path), ctx);
            }
        }
        "pick" | "omit" => {
            if args_len != 2 {
                require(args_len, ExactlyTwo, base_path, ctx);
            } else {
                let allow_terminal_index = expr_op.op == "pick";
                validate_path_array_arg(
                    &expr_op.args[1],
                    &format!("{}.args[1]", base_path),
                    allow_terminal_index,
                    ctx,
                );
            }
        }
        "keys" | "values" | "entries" | "object_flatten" | "object_unflatten" => {
            require(args_len, ExactlyOne, base_path, ctx);
        }
        "from_entries" => {
            require(args_len, OneOrTwo, base_path, ctx);
        }
        "map" | "filter" | "flat_map" | "group_by" | "key_by" | "partition" | "distinct_by"
        | "find" | "find_index" => {
            require(args_len, ExactlyTwo, base_path, ctx);
        }
        "flatten" => {
            require(args_len, OneOrTwo, base_path, ctx);
        }
        "take" | "drop" | "chunk" | "index_of" | "contains" | "reduce" => {
            require(args_len, ExactlyTwo, base_path, ctx);
        }
        "slice" => {
            require(args_len, TwoOrThree, base_path, ctx);
        }
        "zip" => {
            require(args_len, AtLeastTwo, base_path, ctx);
        }
        "zip_with" => {
            require(args_len, AtLeastThree, base_path, ctx);
        }
        "unzip" | "unique" | "sum" | "avg" | "min" | "max" => {
            require(args_len, ExactlyOne, base_path, ctx);
        }
        "sort_by" => {
            require(args_len, TwoOrThree, base_path, ctx);
        }
        "fold" => {
            require(args_len, ExactlyThree, base_path, ctx);
        }
        "concat" | "coalesce" => {
            require(args_len, AtLeastOne, base_path, ctx);
        }
        "+" | "*" => {
            require(args_len, AtLeastTwo, base_path, ctx);
        }
        "-" | "/" | "to_base" => {
            require(args_len, ExactlyTwo, base_path, ctx);
        }
        "round" => {
            require(args_len, OneOrTwo, base_path, ctx);
        }
        "abs" | "floor" | "ceil" | "trunc" | "sqrt" | "sign" => {
            require(args_len, ExactlyOne, base_path, ctx);
        }
        "mod" | "pow" => {
            require(args_len, ExactlyTwo, base_path, ctx);
        }
        "clamp" => {
            require(args_len, ExactlyThree, base_path, ctx);
        }
        "range" => {
            require(args_len, TwoOrThree, base_path, ctx);
        }
        "date_format" => {
            require(args_len, TwoToFour, base_path, ctx);
        }
        "to_unixtime" => {
            require(args_len, OneToThree, base_path, ctx);
        }
        "and" | "or" => {
            require(args_len, AtLeastTwo, base_path, ctx);
        }
        "not" => {
            require(args_len, ExactlyOne, base_path, ctx);
        }
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => {
            require(args_len, ExactlyTwo, base_path, ctx);
        }
        _ => {}
    }
}
