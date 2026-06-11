use super::args::reject_null;
use super::*;

pub(in crate::transform::operators) fn eval_range(
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
    if let Some(max_items) = max_items
        && len > max_items as i128
    {
        return Err(expr_type_error(
            "range length exceeds configured limit",
            path,
        ));
    }
    usize::try_from(len).map_err(|_| expr_type_error("range length is out of bounds", path))
}
