use serde_json::Value as JsonValue;

use super::{EvalValue, V2EvalContext, eval_v2_expr_or_null, value_to_string};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::V2OpStep;

fn value_as_string(value: &JsonValue, path: &str) -> Result<String, TransformError> {
    match value {
        JsonValue::String(value) => Ok(value.clone()),
        _ => Err(
            TransformError::new(TransformErrorKind::ExprError, "value must be a string")
                .with_path(path),
        ),
    }
}

fn value_to_number(value: &JsonValue, path: &str, message: &str) -> Result<f64, TransformError> {
    match value {
        JsonValue::Number(n) => n.as_f64().filter(|f| f.is_finite()).ok_or_else(|| {
            TransformError::new(TransformErrorKind::ExprError, message).with_path(path)
        }),
        JsonValue::String(s) => s
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .ok_or_else(|| {
                TransformError::new(TransformErrorKind::ExprError, message).with_path(path)
            }),
        _ => Err(TransformError::new(TransformErrorKind::ExprError, message).with_path(path)),
    }
}

fn compare_eq_v1(
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

fn compare_numbers_v1<F>(
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

fn match_regex_v1(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
) -> Result<bool, TransformError> {
    let value = value_as_string(left, left_path)?;
    let pattern = value_as_string(right, right_path)?;
    let regex = regex::Regex::new(&pattern).map_err(|e| {
        TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid regex pattern: {}", e),
        )
        .with_path(right_path)
    })?;
    Ok(regex.is_match(&value))
}

pub(super) fn eval_comparison_op<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    if op_step.args.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", path)));
    }
    let left = match pipe_value {
        EvalValue::Missing => JsonValue::Null,
        EvalValue::Value(value) => value,
    };
    let right_path = format!("{}.args[0]", path);
    let right = eval_v2_expr_or_null(&op_step.args[0], record, context, out, &right_path, ctx)?;
    let left_path = path.to_string();
    let op = match op_step.op.as_str() {
        "eq" => "==",
        "ne" => "!=",
        "lt" => "<",
        "lte" => "<=",
        "gt" => ">",
        "gte" => ">=",
        "match" => "~=",
        other => other,
    };
    let result = match op {
        "==" => compare_eq_v1(&left, &right, &left_path, &right_path)?,
        "!=" => !compare_eq_v1(&left, &right, &left_path, &right_path)?,
        "<" => compare_numbers_v1(&left, &right, &left_path, &right_path, |l, r| l < r)?,
        "<=" => compare_numbers_v1(&left, &right, &left_path, &right_path, |l, r| l <= r)?,
        ">" => compare_numbers_v1(&left, &right, &left_path, &right_path, |l, r| l > r)?,
        ">=" => compare_numbers_v1(&left, &right, &left_path, &right_path, |l, r| l >= r)?,
        "~=" => match_regex_v1(&left, &right, &left_path, &right_path)?,
        _ => false,
    };
    Ok(EvalValue::Value(JsonValue::Bool(result)))
}
