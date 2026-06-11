use super::*;

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn eval_unary_number(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    op: fn(f64) -> f64,
) -> Result<EvalValue, TransformError> {
    eval_unary_number_checked(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |value, _| Ok(op(value)),
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn eval_unary_number_checked(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    op: impl FnOnce(f64, &str) -> Result<f64, TransformError>,
) -> Result<EvalValue, TransformError> {
    let (value, path) =
        match eval_one_number_arg(args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    Ok(EvalValue::Value(json_number_from_f64(
        op(value, &path)?,
        base_path,
    )?))
}

pub(super) fn eval_one_number_arg(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Option<(f64, String)>, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }
    eval_number_arg_at(0, args, injected, record, context, out, base_path, locals)
}

pub(super) fn eval_two_number_args(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Option<NumberArgPair>, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }
    let left = match eval_number_arg_at(0, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(None),
        Some(value) => value,
    };
    let right =
        match eval_number_arg_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(None),
            Some(value) => value,
        };
    Ok(Some((left, right)))
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn eval_number_arg_at(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Option<(f64, String)>, TransformError> {
    let value = match eval_arg_value_at(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        None => return Ok(None),
        Some(value) => value,
    };
    let path = format!("{}.args[{}]", base_path, index);
    reject_null(&value, &path)?;
    let number = value_to_number(&value, &path, "operand must be a number")?;
    Ok(Some((number, path)))
}

pub(super) fn reject_null(value: &JsonValue, path: &str) -> Result<(), TransformError> {
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path));
    }
    Ok(())
}
