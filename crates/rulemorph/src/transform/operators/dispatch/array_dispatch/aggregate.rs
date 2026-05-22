use super::super::super::array::{
    eval_array_avg, eval_array_fold, eval_array_max, eval_array_min, eval_array_reduce,
    eval_array_sum,
};
use super::super::*;

pub(super) fn is_array_aggregate_operator(op: &str) -> bool {
    matches!(op, "sum" | "avg" | "min" | "max" | "reduce" | "fold")
}

pub(super) fn eval_array_aggregate_dispatch(
    expr_op: &ExprOp,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    injected: Option<&EvalValue>,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    match expr_op.op.as_str() {
        "sum" => eval_array_sum(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "avg" => eval_array_avg(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "min" => eval_array_min(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "max" => eval_array_max(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "reduce" => eval_array_reduce(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "fold" => eval_array_fold(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        _ => unreachable!("array aggregate dispatch called for non-aggregate operator"),
    }
}
