use rulemorph::v2_eval::{EvalValue, V2EvalContext, eval_v2_condition, eval_v2_expr};
use rulemorph::v2_parser::{parse_v2_condition, parse_v2_expr};
use rulemorph::{Expr, TransformError, TransformErrorKind};
use serde_json::Value as JsonValue;

use super::v2_helpers::{expr_to_json_for_v2_condition, expr_to_json_for_v2_pipe};

pub(super) fn eval_trace_condition(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    rule_version: u8,
) -> Result<bool, TransformError> {
    if rule_version >= 2 {
        if let Some(raw_value) = expr_to_json_for_v2_condition(expr) {
            if let Ok(condition) = parse_v2_condition(&raw_value) {
                let ctx = V2EvalContext::new();
                return eval_v2_condition(&condition, record, context, out, path, &ctx);
            }
            if let Ok(v2_expr) = parse_v2_expr(&raw_value) {
                let ctx = V2EvalContext::new();
                let value = eval_v2_expr(&v2_expr, record, context, out, path, &ctx)?;
                return match value {
                    EvalValue::Missing => Ok(false),
                    EvalValue::Value(JsonValue::Bool(flag)) => Ok(flag),
                    EvalValue::Value(_) => Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "when/record_when must evaluate to boolean",
                    )
                    .with_path(path)),
                };
            }
        }
        if let Some(raw_value) = expr_to_json_for_v2_pipe(expr) {
            let v2_expr = parse_v2_expr(&raw_value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 condition: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new();
            let value = eval_v2_expr(&v2_expr, record, context, out, path, &ctx)?;
            return match value {
                EvalValue::Missing => Ok(false),
                EvalValue::Value(JsonValue::Bool(flag)) => Ok(flag),
                EvalValue::Value(_) => Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "when/record_when must evaluate to boolean",
                )
                .with_path(path)),
            };
        }
    }

    Err(TransformError::new(
        TransformErrorKind::ExprError,
        "when/record_when must evaluate to boolean",
    )
    .with_path(path))
}
