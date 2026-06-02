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

pub(super) fn eval_abs(
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

pub(super) fn eval_floor(
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

pub(super) fn eval_ceil(
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

pub(super) fn eval_trunc(
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

pub(super) fn eval_sqrt(
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

pub(super) fn eval_sign(
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

pub(super) fn eval_mod(
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

pub(super) fn eval_pow(
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

pub(super) fn eval_clamp(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "clamp requires exactly three arguments",
        )
        .with_path(format!("{}.args", base_path)));
    }
    let value =
        match eval_number_arg_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let min = match eval_number_arg_at(1, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let max = match eval_number_arg_at(2, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    if min.0 > max.0 {
        return Err(expr_type_error(
            "clamp min must be less than or equal to max",
            &min.1,
        ));
    }
    Ok(EvalValue::Value(json_number_from_f64(
        value.0.clamp(min.0, max.0),
        base_path,
    )?))
}

pub(super) fn eval_range(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    if injected.is_some() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "range does not accept implicit chain input; use { op: \"range\", args: [start, end, step?] } in v1 or [{ range: [start, end, step?] }] in v2",
        )
        .with_path(format!("{}.args", base_path)));
    }
    let total_len = args.len();
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "range requires two or three explicit arguments; use { op: \"range\", args: [start, end, step?] } in v1 or [{ range: [start, end, step?] }] in v2",
        )
        .with_path(format!("{}.args", base_path)));
    }
    let start_value =
        match eval_arg_value_at(0, args, None, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let end_value = match eval_arg_value_at(1, args, None, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let start_path = format!("{}.args[0]", base_path);
    let end_path = format!("{}.args[1]", base_path);
    reject_null(&start_value, &start_path)?;
    reject_null(&end_value, &end_path)?;
    let start = value_to_i64(&start_value, &start_path, "range start must be an integer")?;
    let end = value_to_i64(&end_value, &end_path, "range end must be an integer")?;
    let step = if total_len == 3 {
        let step_value =
            match eval_arg_value_at(2, args, None, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        let step_path = format!("{}.args[2]", base_path);
        reject_null(&step_value, &step_path)?;
        value_to_i64(&step_value, &step_path, "range step must be an integer")?
    } else if start <= end {
        1
    } else {
        -1
    };
    let limits = locals.map(|locals| locals.limits).unwrap_or_default();
    let len = checked_range_len(start, end, step, limits.max_range_items, base_path)?;
    limits.check_generated_array_items(len, base_path)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| expr_type_error("range allocation failed", base_path))?;
    let mut current = start;
    for index in 0..len {
        values.push(JsonValue::Number(current.into()));
        if index + 1 < len {
            current = current
                .checked_add(step)
                .ok_or_else(|| expr_type_error("range value is out of bounds", base_path))?;
        }
    }
    Ok(EvalValue::Value(JsonValue::Array(values)))
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

#[allow(clippy::too_many_arguments)]
fn eval_unary_number(
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

#[allow(clippy::too_many_arguments)]
fn eval_unary_number_checked(
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

#[allow(clippy::too_many_arguments)]
fn eval_one_number_arg(
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

#[allow(clippy::too_many_arguments)]
fn eval_two_number_args(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Option<((f64, String), (f64, String))>, TransformError> {
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

#[allow(clippy::too_many_arguments)]
fn eval_number_arg_at(
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

fn reject_null(value: &JsonValue, path: &str) -> Result<(), TransformError> {
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path));
    }
    Ok(())
}

fn checked_range_len(
    start: i64,
    end: i64,
    step: i64,
    max_items: Option<usize>,
    path: &str,
) -> Result<usize, TransformError> {
    if step == 0 {
        return Err(expr_type_error("range step must not be zero", path));
    }
    if (step > 0 && start >= end) || (step < 0 && start <= end) {
        return Ok(0);
    }

    let distance = if step > 0 {
        i128::from(end) - i128::from(start)
    } else {
        i128::from(start) - i128::from(end)
    };
    let step_abs = i128::from(step).abs();
    let len = (distance + step_abs - 1) / step_abs;
    if let Some(max_items) = max_items {
        if len > max_items as i128 {
            return Err(expr_type_error(
                "range length exceeds configured limit",
                path,
            ));
        }
    }
    usize::try_from(len).map_err(|_| expr_type_error("range length is out of bounds", path))
}
