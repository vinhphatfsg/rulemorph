use std::collections::HashMap;

use anyhow::{Result, anyhow};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use rulemorph::v2_eval::{EvalValue, V2EvalContext, eval_v2_expr};
use rulemorph::{Mapping, RuleFile, TransformError, transform_record};
use serde_json::Value as JsonValue;

use super::error::EndpointError;

pub(super) fn build_headers(
    headers: &HashMap<String, rulemorph::v2_model::V2Expr>,
    input: &JsonValue,
    context: Option<&JsonValue>,
) -> Result<HeaderMap, EndpointError> {
    let mut map = HeaderMap::new();
    for (key, expr) in headers {
        let lower = key.trim().to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "host" | "forwarded" | "x-forwarded-for" | "x-forwarded-host" | "x-forwarded-proto"
        ) {
            return Err(EndpointError::invalid(format!(
                "disallowed header: {}",
                key
            )));
        }
        let value = match eval_expr_value(expr, input, context)
            .map_err(|err| EndpointError::invalid(format!("expr eval error: {}", err)))?
        {
            EvalValue::Missing => {
                continue;
            }
            EvalValue::Value(JsonValue::String(value)) => value,
            EvalValue::Value(other) => {
                return Err(EndpointError::invalid(format!(
                    "expected string, got {}",
                    json_value_kind(&other)
                )));
            }
        };
        let name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| EndpointError::invalid("invalid header name"))?;
        let header_value = HeaderValue::from_str(&value)
            .map_err(|_| EndpointError::invalid("invalid header value"))?;
        map.insert(name, header_value);
    }
    Ok(map)
}

pub(super) fn apply_mappings_via_rule(
    mappings: &[Mapping],
    record: &JsonValue,
    context: Option<&JsonValue>,
) -> Result<Option<JsonValue>, TransformError> {
    let rule = RuleFile {
        version: 2,
        input: rulemorph::InputSpec {
            format: rulemorph::InputFormat::Json,
            csv: None,
            json: None,
            yaml: None,
            toml: None,
            xml: None,
            html: None,
            excel: None,
            markdown: None,
        },
        defs: Default::default(),
        codecs: Default::default(),
        output: None,
        record_when: None,
        mappings: mappings.to_vec(),
        steps: None,
        finalize: None,
    };
    transform_record(&rule, record, context)
}

pub(super) fn eval_expr_value(
    expr: &rulemorph::v2_model::V2Expr,
    input: &JsonValue,
    context: Option<&JsonValue>,
) -> Result<EvalValue> {
    let ctx = V2EvalContext::new();
    let empty = JsonValue::Object(Default::default());
    eval_v2_expr(expr, input, context, &empty, "expr", &ctx).map_err(|err| anyhow!(err.to_string()))
}

pub(super) fn eval_expr_string(
    expr: &rulemorph::v2_model::V2Expr,
    input: &JsonValue,
    context: Option<&JsonValue>,
) -> Result<String, EndpointError> {
    match eval_expr_value(expr, input, context)
        .map_err(|err| EndpointError::invalid(format!("expr eval error: {}", err)))?
    {
        EvalValue::Missing => Err(EndpointError::invalid("expected string, got missing")),
        EvalValue::Value(JsonValue::String(value)) => Ok(value),
        EvalValue::Value(other) => Err(EndpointError::invalid(format!(
            "expected string, got {}",
            json_value_kind(&other)
        ))),
    }
}

fn json_value_kind(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}
