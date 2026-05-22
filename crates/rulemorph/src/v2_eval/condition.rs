use serde_json::Value as JsonValue;

use super::{EvalValue, V2EvalContext, eval_v2_expr};
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_model::{V2Comparison, V2ComparisonOp, V2Condition};

/// Evaluate a v2 condition - returns bool
pub fn eval_v2_condition<'a>(
    condition: &V2Condition,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<bool, TransformError> {
    match condition {
        V2Condition::All(conditions) => {
            for (i, cond) in conditions.iter().enumerate() {
                let cond_path = format!("{}[{}]", path, i);
                if !eval_v2_condition(cond, record, context, out, &cond_path, ctx)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        V2Condition::Any(conditions) => {
            for (i, cond) in conditions.iter().enumerate() {
                let cond_path = format!("{}[{}]", path, i);
                if eval_v2_condition(cond, record, context, out, &cond_path, ctx)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        V2Condition::Comparison(comparison) => {
            eval_v2_comparison(comparison, record, context, out, path, ctx)
        }
        V2Condition::Expr(expr) => {
            let expr_path = format!("{}.expr", path);
            let value = eval_v2_expr(expr, record, context, out, &expr_path, ctx)?;
            match value {
                EvalValue::Value(JsonValue::Bool(flag)) => Ok(flag),
                EvalValue::Missing => Ok(false),
                EvalValue::Value(_) => Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "when/record_when must evaluate to boolean",
                )
                .with_path(&expr_path)),
            }
        }
    }
}

/// Evaluate a v2 comparison
fn eval_v2_comparison<'a>(
    comparison: &V2Comparison,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<bool, TransformError> {
    if comparison.args.len() != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!(
                "comparison requires exactly 2 arguments, got {}",
                comparison.args.len()
            ),
        )
        .with_path(path));
    }

    let left_path = format!("{}.args[0]", path);
    let right_path = format!("{}.args[1]", path);

    let left = eval_v2_expr(&comparison.args[0], record, context, out, &left_path, ctx)?;
    let right = eval_v2_expr(&comparison.args[1], record, context, out, &right_path, ctx)?;

    match comparison.op {
        V2ComparisonOp::Eq => Ok(compare_values_eq(&left, &right)),
        V2ComparisonOp::Ne => Ok(!compare_values_eq(&left, &right)),
        V2ComparisonOp::Gt => {
            compare_values_ord(&left, &right, path).map(|ord| ord == std::cmp::Ordering::Greater)
        }
        V2ComparisonOp::Gte => {
            compare_values_ord(&left, &right, path).map(|ord| ord != std::cmp::Ordering::Less)
        }
        V2ComparisonOp::Lt => {
            compare_values_ord(&left, &right, path).map(|ord| ord == std::cmp::Ordering::Less)
        }
        V2ComparisonOp::Lte => {
            compare_values_ord(&left, &right, path).map(|ord| ord != std::cmp::Ordering::Greater)
        }
        V2ComparisonOp::Match => compare_values_match(&left, &right, path),
    }
}

/// Compare two values for equality
pub(super) fn compare_values_eq(left: &EvalValue, right: &EvalValue) -> bool {
    match (left, right) {
        (EvalValue::Value(l), EvalValue::Value(r)) => l == r,
        (EvalValue::Missing, EvalValue::Missing) => true,
        (EvalValue::Missing, EvalValue::Value(r)) => r.is_null(),
        (EvalValue::Value(l), EvalValue::Missing) => l.is_null(),
    }
}

/// Compare two values for ordering
fn compare_values_ord(
    left: &EvalValue,
    right: &EvalValue,
    path: &str,
) -> Result<std::cmp::Ordering, TransformError> {
    match (left, right) {
        (EvalValue::Value(l), EvalValue::Value(r)) => {
            // Try numeric comparison first
            if let (Some(l_num), Some(r_num)) = (value_as_f64(l), value_as_f64(r)) {
                return Ok(l_num
                    .partial_cmp(&r_num)
                    .unwrap_or(std::cmp::Ordering::Equal));
            }
            // Try string comparison
            if let (Some(l_str), Some(r_str)) = (value_as_str(l), value_as_str(r)) {
                return Ok(l_str.cmp(r_str));
            }
            Err(TransformError::new(
                TransformErrorKind::ExprError,
                "cannot compare values of different types",
            )
            .with_path(path))
        }
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "cannot compare missing values",
        )
        .with_path(path)),
    }
}

/// Compare with regex match
fn compare_values_match(
    left: &EvalValue,
    right: &EvalValue,
    path: &str,
) -> Result<bool, TransformError> {
    let text = match left {
        EvalValue::Value(JsonValue::String(s)) => s.as_str(),
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "match operator requires string on left side",
            )
            .with_path(path));
        }
    };

    let pattern = match right {
        EvalValue::Value(JsonValue::String(s)) => s.as_str(),
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "match operator requires regex pattern string on right side",
            )
            .with_path(path));
        }
    };

    let re = regex::Regex::new(pattern).map_err(|e| {
        TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid regex pattern: {}", e),
        )
        .with_path(path)
    })?;

    Ok(re.is_match(text))
}

/// Helper to get f64 from JsonValue
fn value_as_f64(v: &JsonValue) -> Option<f64> {
    match v {
        JsonValue::Number(n) => n.as_f64(),
        JsonValue::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// Helper to get str from JsonValue
fn value_as_str(v: &JsonValue) -> Option<&str> {
    match v {
        JsonValue::String(s) => Some(s.as_str()),
        _ => None,
    }
}
