use super::*;

pub(super) fn eval_wrap_value(
    value: &JsonValue,
    out: &JsonValue,
    context: Option<&JsonValue>,
    path: &str,
    limits: EvalLimits,
) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Object(map) => {
            let mut out_map = serde_json::Map::new();
            for (key, value) in map {
                let child_path = format!("{}.{}", path, key);
                out_map.insert(
                    key.clone(),
                    eval_wrap_value(value, out, context, &child_path, limits)?,
                );
            }
            Ok(JsonValue::Object(out_map))
        }
        _ => {
            let expr = parse_v2_expr(value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 expr: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new().with_limits(limits);
            match eval_v2_expr(&expr, out, context, out, path, &ctx)? {
                V2EvalValue::Missing => Ok(JsonValue::Null),
                V2EvalValue::Value(value) => Ok(value),
            }
        }
    }
}
