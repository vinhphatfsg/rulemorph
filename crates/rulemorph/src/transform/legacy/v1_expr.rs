use super::super::*;

mod chain;
mod ref_resolution;
mod when;

pub(in crate::transform) use chain::eval_chain;
pub(in crate::transform) use ref_resolution::{canonical_ref_path, eval_ref, resolve_source};
pub(in crate::transform) use when::{
    MappingWhenInput, TracedMappingWhenInput, eval_record_when, eval_record_when_traced, eval_when,
    eval_when_expr_traced_with_v2_context, eval_when_expr_with_v2_context, eval_when_traced,
};

pub(in crate::transform) fn eval_expr(
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
