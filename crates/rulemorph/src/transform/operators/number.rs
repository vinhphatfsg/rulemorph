use super::*;

pub(super) fn eval_numeric_op(
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

pub(super) fn eval_round(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=2).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one or two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_value_at(0, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let value_path = format!("{}.args[0]", base_path);
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(value_path));
    }
    let number = value_to_number(&value, &value_path, "operand must be a number")?;

    let scale = if total_len == 2 {
        let scale_path = format!("{}.args[1]", base_path);
        let scale_value =
            match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if scale_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(scale_path));
        }
        let scale = value_to_i64(
            &scale_value,
            &scale_path,
            "scale must be a non-negative integer",
        )?;
        if scale < 0 {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "scale must be a non-negative integer",
            )
            .with_path(scale_path));
        }
        if scale > 308 {
            return Err(
                TransformError::new(TransformErrorKind::ExprError, "scale is too large")
                    .with_path(scale_path),
            );
        }
        scale as i32
    } else {
        0
    };

    let rounded = if scale == 0 {
        number.round()
    } else {
        let factor = 10f64.powi(scale);
        (number * factor).round() / factor
    };

    Ok(EvalValue::Value(json_number_from_f64(rounded, base_path)?))
}

pub(super) fn eval_to_base(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_value_at(0, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let base_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let value_path = format!("{}.args[0]", base_path);
    let base_path_arg = format!("{}.args[1]", base_path);
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(value_path));
    }
    if base_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path_arg));
    }

    let number = value_to_i64(&value, &value_path, "value must be an integer")?;
    let base = value_to_i64(&base_value, &base_path_arg, "base must be an integer")?;
    if !(2..=36).contains(&base) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "base must be between 2 and 36",
        )
        .with_path(base_path_arg));
    }

    let formatted = to_radix_string(number, base as u32, &value_path)?;
    Ok(EvalValue::Value(JsonValue::String(formatted)))
}
