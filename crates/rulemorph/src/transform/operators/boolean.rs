use super::*;

#[expect(
    clippy::too_many_arguments,
    reason = "operator eval helpers keep the shared v1 expression call shape until a wider evaluator context rewrite"
)]
pub(super) fn eval_bool_and_or(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    is_and: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut saw_missing = false;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index(
            index, args, injected, record, context, out, base_path, locals,
        )?;
        match value {
            EvalValue::Missing => {
                saw_missing = true;
                continue;
            }
            EvalValue::Value(value) => {
                let flag = value_as_bool(&value, &arg_path)?;
                if is_and {
                    if !flag {
                        return Ok(EvalValue::Value(JsonValue::Bool(false)));
                    }
                } else if flag {
                    return Ok(EvalValue::Value(JsonValue::Bool(true)));
                }
            }
        }
    }

    if saw_missing {
        Ok(EvalValue::Missing)
    } else {
        Ok(EvalValue::Value(JsonValue::Bool(is_and)))
    }
}

pub(super) fn eval_bool_not(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value = eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    match value {
        EvalValue::Missing => Ok(EvalValue::Missing),
        EvalValue::Value(value) => {
            let flag = value_as_bool(&value, &arg_path)?;
            Ok(EvalValue::Value(JsonValue::Bool(!flag)))
        }
    }
}

pub(super) fn eval_compare(
    expr_op: &ExprOp,
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(&expr_op.args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let left_path = format!("{}.args[0]", base_path);
    let right_path = format!("{}.args[1]", base_path);
    let left = eval_expr_value_or_null_at(
        0,
        &expr_op.args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
    )?;
    let right = eval_expr_value_or_null_at(
        1,
        &expr_op.args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
    )?;

    let result = match expr_op.op.as_str() {
        "==" => compare_eq(&left, &right, &left_path, &right_path)?,
        "!=" => !compare_eq(&left, &right, &left_path, &right_path)?,
        "<" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l < r)?,
        "<=" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l <= r)?,
        ">" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l > r)?,
        ">=" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l >= r)?,
        "~=" => match_regex(&left, &right, &left_path, &right_path)?,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr.op is not supported",
            )
            .with_path(format!("{}.op", base_path)));
        }
    };

    Ok(EvalValue::Value(JsonValue::Bool(result)))
}

pub(super) fn compare_eq(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
) -> Result<bool, TransformError> {
    if left.is_null() || right.is_null() {
        return Ok(left.is_null() && right.is_null());
    }

    let left_value = value_to_string(left, left_path)?;
    let right_value = value_to_string(right, right_path)?;
    Ok(left_value == right_value)
}

fn compare_numbers<F>(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
    compare: F,
) -> Result<bool, TransformError>
where
    F: FnOnce(f64, f64) -> bool,
{
    let left_value = value_to_number(left, left_path, "comparison operand must be a number")?;
    let right_value = value_to_number(right, right_path, "comparison operand must be a number")?;
    Ok(compare(left_value, right_value))
}

fn match_regex(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
) -> Result<bool, TransformError> {
    let value = value_as_string(left, left_path)?;
    let pattern = value_as_string(right, right_path)?;
    let regex = cached_regex(&pattern, right_path)?;
    Ok(regex.is_match(&value))
}
