use super::*;

mod chain;
mod ref_resolution;
mod when;

pub(super) use chain::eval_chain;
pub(super) use ref_resolution::{canonical_ref_path, eval_ref, resolve_source};
pub(super) use when::{
    eval_record_when, eval_record_when_traced, eval_when, eval_when_expr, eval_when_expr_traced,
    eval_when_traced,
};

pub(super) fn eval_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    match expr {
        Expr::Literal(value) => Ok(EvalValue::Value(value.clone())),
        Expr::Ref(expr_ref) => eval_ref(expr_ref, record, context, out, base_path, locals),
        Expr::Op(expr_op) => eval_op(expr_op, record, context, out, base_path, None, locals),
        Expr::Chain(expr_chain) => eval_chain(expr_chain, record, context, out, base_path, locals),
    }
}
