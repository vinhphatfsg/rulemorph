use super::args::eval_two_number_args;
use super::*;

pub(in crate::transform::operators) fn eval_numeric_op(
    expr_op: &ExprOp,
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let op = expr_op.op.as_str();
    let args = &expr_op.args;
    let total_len = args_len(args, injected);

    let requires_exact_two = matches!(op, "-" | "/");
    if requires_exact_two && total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }
    if !requires_exact_two && total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut result: f64 = 0.0;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = match eval_arg_value_at(
            index, args, injected, record, context, out, base_path, locals,
        )? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
        if value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(arg_path));
        }
        let number = value_to_number(&value, &arg_path, "operand must be a number")?;
        if index == 0 {
            result = number;
        } else {
            result = match op {
                "+" => result + number,
                "-" => result - number,
                "*" => result * number,
                "/" => result / number,
                _ => result,
            };
        }
    }

    Ok(EvalValue::Value(json_number_from_f64(result, base_path)?))
}

pub(in crate::transform::operators) fn eval_mod(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let (lhs, rhs) =
        match eval_two_number_args(args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(values) => values,
        };
    if rhs.0 == 0.0 {
        return Err(expr_type_error("mod divisor must not be zero", &rhs.1));
    }
    let result = lhs.0.rem_euclid(rhs.0.abs());
    Ok(EvalValue::Value(json_number_from_f64(result, base_path)?))
}

pub(in crate::transform::operators) fn eval_pow(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let (base, exponent) =
        match eval_two_number_args(args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(values) => values,
        };
    let result = base.0.powf(exponent.0);
    Ok(EvalValue::Value(json_number_from_f64(result, base_path)?))
}
