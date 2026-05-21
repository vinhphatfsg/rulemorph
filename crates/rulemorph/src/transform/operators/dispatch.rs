use super::args::args_len;
use super::boolean::{eval_bool_and_or, eval_bool_not, eval_compare};
use super::date::{eval_date_format, eval_to_unixtime};
use super::lookup::eval_lookup;
use super::number::{eval_numeric_op, eval_round, eval_to_base};
use super::*;

mod array_dispatch;
mod json_dispatch;
mod string_dispatch;

use self::array_dispatch::{eval_array_dispatch, is_array_operator};
use self::json_dispatch::{eval_json_dispatch, is_json_operator};
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
        op if is_json_operator(op) => {
            eval_json_dispatch(expr_op, record, context, out, base_path, injected, locals)
        }
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
