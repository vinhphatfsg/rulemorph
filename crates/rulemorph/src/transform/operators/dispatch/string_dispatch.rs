use super::super::simple::{eval_coalesce, eval_concat};
use super::super::string::{eval_pad, eval_replace, eval_split, eval_unary_string_op};
use super::super::value::{value_as_string, value_to_string};
use super::super::*;

pub(super) fn is_string_operator(op: &str) -> bool {
    matches!(
        op,
        "concat"
            | "coalesce"
            | "to_string"
            | "trim"
            | "lowercase"
            | "uppercase"
            | "replace"
            | "split"
            | "pad_start"
            | "pad_end"
    )
}

pub(super) fn eval_string_dispatch(
    expr_op: &ExprOp,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    injected: Option<&EvalValue>,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    match expr_op.op.as_str() {
        "concat" => eval_concat(expr_op, injected, record, context, out, base_path, locals),
        "coalesce" => eval_coalesce(expr_op, injected, record, context, out, base_path, locals),
        "to_string" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| value_to_string(value, path).map(JsonValue::String),
        ),
        "trim" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.trim().to_string()))
            },
        ),
        "lowercase" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.to_lowercase()))
            },
        ),
        "uppercase" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.to_uppercase()))
            },
        ),
        "replace" => eval_replace(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "split" => eval_split(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "pad_start" => eval_pad(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "pad_end" => eval_pad(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        _ => unreachable!("string dispatch called for non-string operator"),
    }
}
