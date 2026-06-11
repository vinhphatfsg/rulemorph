use super::args::{eval_one_number_arg, eval_unary_number, eval_unary_number_checked};
use super::*;

pub(in crate::transform::operators) fn eval_abs(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_unary_number(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        f64::abs,
    )
}

pub(in crate::transform::operators) fn eval_floor(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_unary_number(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        f64::floor,
    )
}

pub(in crate::transform::operators) fn eval_ceil(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_unary_number(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        f64::ceil,
    )
}

pub(in crate::transform::operators) fn eval_trunc(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_unary_number(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        f64::trunc,
    )
}

pub(in crate::transform::operators) fn eval_sqrt(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_unary_number_checked(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |value, path| {
            if value < 0.0 {
                return Err(expr_type_error("sqrt operand must be non-negative", path));
            }
            Ok(value.sqrt())
        },
    )
}

pub(in crate::transform::operators) fn eval_sign(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let value = match eval_one_number_arg(args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some((value, _)) => value,
    };
    let sign = if value < 0.0 {
        -1
    } else if value > 0.0 {
        1
    } else {
        0
    };
    Ok(EvalValue::Value(JsonValue::Number(sign.into())))
}
