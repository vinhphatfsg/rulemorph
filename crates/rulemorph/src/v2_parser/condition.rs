use crate::v2_model::{V2Comparison, V2ComparisonOp, V2Condition};
use serde_json::Value as JsonValue;

use super::{V2ParseError, parse_v2_expr, parse_v2_expr_args};

/// Parse a V2Condition from a JSON value
pub fn parse_v2_condition(value: &JsonValue) -> Result<V2Condition, V2ParseError> {
    match value {
        JsonValue::Object(obj) => {
            // Check for all: [...]
            if let Some(all_arr) = obj.get("all") {
                return parse_condition_array(all_arr, V2Condition::All);
            }
            // Check for any: [...]
            if let Some(any_arr) = obj.get("any") {
                return parse_condition_array(any_arr, V2Condition::Any);
            }
            // Check for comparison operators: eq, ne, gt, gte, lt, lte, match
            if let Some(comp) = parse_comparison_from_object(obj)? {
                return Ok(V2Condition::Comparison(comp));
            }
            // Otherwise, treat as expression condition
            let expr = parse_v2_expr(value)?;
            Ok(V2Condition::Expr(expr))
        }
        JsonValue::Array(_) => {
            // Array treated as expression
            let expr = parse_v2_expr(value)?;
            Ok(V2Condition::Expr(expr))
        }
        _ => {
            // Other values treated as expression
            let expr = parse_v2_expr(value)?;
            Ok(V2Condition::Expr(expr))
        }
    }
}

fn parse_condition_array<F>(value: &JsonValue, constructor: F) -> Result<V2Condition, V2ParseError>
where
    F: FnOnce(Vec<V2Condition>) -> V2Condition,
{
    match value {
        JsonValue::Array(arr) => {
            let conditions: Result<Vec<V2Condition>, _> =
                arr.iter().map(parse_v2_condition).collect();
            Ok(constructor(conditions?))
        }
        _ => Err(V2ParseError::InvalidCondition(
            "all/any must contain an array".to_string(),
        )),
    }
}

fn parse_comparison_from_object(
    obj: &serde_json::Map<String, JsonValue>,
) -> Result<Option<V2Comparison>, V2ParseError> {
    let ops = [
        ("eq", V2ComparisonOp::Eq),
        ("ne", V2ComparisonOp::Ne),
        ("gt", V2ComparisonOp::Gt),
        ("gte", V2ComparisonOp::Gte),
        ("lt", V2ComparisonOp::Lt),
        ("lte", V2ComparisonOp::Lte),
        ("match", V2ComparisonOp::Match),
    ];

    for (key, op) in ops.iter() {
        if let Some(args_val) = obj.get(*key) {
            let args = parse_v2_expr_args(args_val)?;
            return Ok(Some(V2Comparison { op: *op, args }));
        }
    }

    Ok(None)
}
