use super::*;

pub(super) fn eval_wrap_value(
    value: &JsonValue,
    out: &JsonValue,
    context: Option<&JsonValue>,
    path: &str,
    ctx: &V2EvalContext<'_>,
) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Object(map) => {
            let mut out_map = serde_json::Map::new();
            for (key, value) in map {
                let child_path = format!("{}.{}", path, key);
                out_map.insert(
                    key.clone(),
                    eval_wrap_value(value, out, context, &child_path, ctx)?,
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
            match eval_v2_expr(&expr, out, context, out, path, &ctx)? {
                V2EvalValue::Missing => Ok(JsonValue::Null),
                V2EvalValue::Value(value) => Ok(value),
            }
        }
    }
}

pub(super) fn eval_wrap_value_traced(
    value: &JsonValue,
    out: &JsonValue,
    context: Option<&JsonValue>,
    path: &str,
    ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Object(map) => {
            let mut out_map = serde_json::Map::new();
            for (key, value) in map {
                let child_path = format!("{}.{}", path, key);
                out_map.insert(
                    key.clone(),
                    eval_wrap_value_traced(value, out, context, &child_path, ctx, collector)?,
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
            match eval_v2_expr_traced(&expr, out, context, out, path, ctx, collector)? {
                V2EvalValue::Missing => Ok(JsonValue::Null),
                V2EvalValue::Value(value) => Ok(value),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wrap_custom_op_calls_share_limit_across_leaf_values() {
        let rule = crate::parse_rule_file(
            r#"
version: 2
input:
  format: json
  json: {}
defs:
  id:
    input: int
    returns: int
    expr: "$"
mappings: []
finalize:
  wrap:
    a: ["@out", { map: [id] }]
    b: ["@out", { map: [id] }]
"#,
        )
        .expect("rule parses");
        let finalize = rule.finalize.as_ref().expect("finalize exists");
        let limits = EvalLimits {
            max_custom_op_calls_per_record: 3,
            ..EvalLimits::default()
        };

        let err = apply_finalize(&rule, finalize, json!([1, 2]), None, limits)
            .expect_err("wrap leaves share one custom-op call budget");

        assert!(
            err.message
                .contains("custom op calls per record exceed configured limit")
        );
    }
}
