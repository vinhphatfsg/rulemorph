use anyhow::{Context, Result, anyhow};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use rulemorph::v2_eval::EvalValue;
use serde_json::{Value as JsonValue, json};

use super::EndpointEngine;
use super::config::RequestContext;
use super::endpoint_rule::CompiledReply;
use super::error::EndpointError;
use super::expr::eval_expr_value;

impl EndpointEngine {
    pub(super) fn build_reply(
        &self,
        reply: &CompiledReply,
        input: &JsonValue,
        context: &JsonValue,
    ) -> Result<Response> {
        let status_value = eval_expr_value(&reply.status, input, Some(context))?;
        let status = match status_value {
            EvalValue::Value(JsonValue::Number(num)) => num
                .as_u64()
                .ok_or_else(|| anyhow!("status must be integer"))?,
            EvalValue::Value(JsonValue::String(s)) => s
                .parse::<u64>()
                .map_err(|_| anyhow!("status must be integer"))?,
            _ => return Err(anyhow!("status must be integer")),
        };
        if !(100..=599).contains(&status) {
            return Err(anyhow!("status out of range"));
        }
        let status = StatusCode::from_u16(status as u16).context("invalid status")?;

        let body = if let Some(body_expr) = &reply.body {
            match eval_expr_value(body_expr, input, Some(context))? {
                EvalValue::Missing => Some(JsonValue::Null),
                EvalValue::Value(value) => Some(value),
            }
        } else {
            None
        };

        let mut headers = HeaderMap::new();
        for (key, value) in &reply.headers {
            let name = HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| anyhow!("invalid header name"))?;
            let header_value =
                HeaderValue::from_str(value).map_err(|_| anyhow!("invalid header value"))?;
            headers.insert(name, header_value);
        }
        if body.is_some() && !headers.contains_key("content-type") {
            headers.insert(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            );
        }

        let mut response = if let Some(body) = &body {
            Response::new(axum::body::Body::from(
                serde_json::to_vec(body).unwrap_or_else(|_| b"null".to_vec()),
            ))
        } else {
            Response::new(axum::body::Body::empty())
        };
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        Ok(response)
    }

    pub(super) fn build_context_json(&self, request_context: Option<&RequestContext>) -> JsonValue {
        let mut value = json!({
            "config": {
                "internal_base": self.config.internal_base,
            }
        });
        if let Some(request_context) = request_context
            && let Some(tenant_id) = request_context.tenant_id.as_ref()
            && let JsonValue::Object(ref mut map) = value
        {
            map.insert(
                "tenant_id".to_string(),
                JsonValue::String(tenant_id.clone()),
            );
        }
        value
    }

    pub(super) fn step_context(
        &self,
        base_context: &JsonValue,
        params: Option<&JsonValue>,
        error: Option<&EndpointError>,
    ) -> JsonValue {
        let mut value = base_context.clone();
        if let Some(params) = params
            && let JsonValue::Object(ref mut map) = value
        {
            map.insert("params".to_string(), params.clone());
        }
        if let Some(error) = error
            && let JsonValue::Object(ref mut map) = value
        {
            map.insert("error".to_string(), error.to_json());
        }
        value
    }
}
