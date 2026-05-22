use super::*;

pub(in crate::transform) fn eval_chain(
    expr_chain: &ExprChain,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    if expr_chain.chain.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.chain must be a non-empty array",
        )
        .with_path(format!("{}.chain", base_path)));
    }

    let first_path = format!("{}.chain[0]", base_path);
    let mut current = eval_expr(
        &expr_chain.chain[0],
        record,
        context,
        out,
        &first_path,
        locals,
    )?;

    for (index, step) in expr_chain.chain.iter().enumerate().skip(1) {
        let step_path = format!("{}.chain[{}]", base_path, index);
        let expr_op = match step {
            Expr::Op(expr_op) => expr_op,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr.chain items after first must be op",
                )
                .with_path(step_path));
            }
        };

        let injected = current.clone();
        current = eval_op(
            expr_op,
            record,
            context,
            out,
            &step_path,
            Some(&injected),
            locals,
        )?;
    }

    Ok(current)
}
