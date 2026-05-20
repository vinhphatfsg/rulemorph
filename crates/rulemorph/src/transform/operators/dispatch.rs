use super::args::args_len;
use super::boolean::{eval_bool_and_or, eval_bool_not, eval_compare};
use super::date::{eval_date_format, eval_to_unixtime};
use super::json::{
    eval_json_entries, eval_json_from_entries, eval_json_get, eval_json_keys, eval_json_merge,
    eval_json_object_flatten, eval_json_object_unflatten, eval_json_omit, eval_json_pick,
    eval_json_values, eval_len,
};
use super::lookup::eval_lookup;
use super::number::{eval_numeric_op, eval_round, eval_to_base};
use super::*;

mod array_dispatch;
mod string_dispatch;

use self::array_dispatch::{eval_array_dispatch, is_array_operator};
use self::string_dispatch::{eval_string_dispatch, is_string_operator};

pub(crate) fn eval_op(
    expr_op: &ExprOp,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    injected: Option<&EvalValue>,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(&expr_op.args, injected);
    if total_len == 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must be a non-empty array",
        )
        .with_path(format!("{}.args", base_path)));
    }

    match expr_op.op.as_str() {
        op if is_string_operator(op) => {
            eval_string_dispatch(expr_op, record, context, out, base_path, injected, locals)
        }
        "lookup" => eval_lookup(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "lookup_first" => eval_lookup(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "merge" => eval_json_merge(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "deep_merge" => eval_json_merge(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "get" => eval_json_get(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "pick" => eval_json_pick(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "omit" => eval_json_omit(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "keys" => eval_json_keys(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "values" => eval_json_values(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "entries" => eval_json_entries(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "len" => eval_len(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "from_entries" => eval_json_from_entries(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "object_flatten" => eval_json_object_flatten(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "object_unflatten" => eval_json_object_unflatten(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        op if is_array_operator(op) => {
            eval_array_dispatch(expr_op, record, context, out, base_path, injected, locals)
        }
        "+" | "-" | "*" | "/" => {
            eval_numeric_op(expr_op, injected, record, context, out, base_path, locals)
        }
        "round" => eval_round(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "to_base" => eval_to_base(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "date_format" => eval_date_format(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "to_unixtime" => eval_to_unixtime(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "and" => eval_bool_and_or(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "or" => eval_bool_and_or(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "not" => eval_bool_not(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => {
            eval_compare(expr_op, injected, record, context, out, base_path, locals)
        }
        _ => Err(
            TransformError::new(TransformErrorKind::ExprError, "expr.op is not supported")
                .with_path(format!("{}.op", base_path)),
        ),
    }
}
