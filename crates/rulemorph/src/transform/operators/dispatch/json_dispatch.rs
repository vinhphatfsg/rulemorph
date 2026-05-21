use super::super::json::{
    eval_json_entries, eval_json_from_entries, eval_json_get, eval_json_keys, eval_json_merge,
    eval_json_object_flatten, eval_json_object_unflatten, eval_json_omit, eval_json_pick,
    eval_json_values, eval_len,
};
use super::*;

pub(super) fn is_json_operator(op: &str) -> bool {
    matches!(
        op,
        "merge"
            | "deep_merge"
            | "get"
            | "pick"
            | "omit"
            | "keys"
            | "values"
            | "entries"
            | "len"
            | "from_entries"
            | "object_flatten"
            | "object_unflatten"
    )
}

pub(super) fn eval_json_dispatch(
    expr_op: &ExprOp,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    injected: Option<&EvalValue>,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    match expr_op.op.as_str() {
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
        _ => unreachable!("json dispatch called for non-json operator"),
    }
}
