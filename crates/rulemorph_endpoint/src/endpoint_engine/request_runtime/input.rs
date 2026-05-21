use anyhow::{Result, anyhow};
use axum::body::Body;
use axum::http::Method;
use axum::http::request::Parts;
use serde_json::Value as JsonValue;

use super::super::endpoint_rule::EndpointMatch;
use super::super::error::EndpointError;
use super::super::expr::apply_mappings_via_rule;
use super::super::request_body::read_request_body;
use super::super::request_input::{build_input, build_input_from_parts, parse_query};
use super::super::{EndpointEngine, empty_object};

pub(super) struct PreparedRequestInput {
    pub(super) record_input: JsonValue,
    pub(super) current: JsonValue,
    pub(super) record_status: String,
    pub(super) record_error: Option<JsonValue>,
    pub(super) last_error_message: Option<String>,
    pub(super) skip_steps: bool,
    pub(super) multipart_temp_dir: Option<tempfile::TempDir>,
}

impl PreparedRequestInput {
    fn new() -> Self {
        Self {
            record_input: JsonValue::Null,
            current: JsonValue::Null,
            record_status: "ok".to_string(),
            record_error: None,
            last_error_message: None,
            skip_steps: false,
            multipart_temp_dir: None,
        }
    }

    fn set_result(&mut self, record_input: JsonValue, current: JsonValue) {
        self.record_input = record_input;
        self.current = current;
    }

    fn set_error(&mut self, engine: &EndpointEngine, err: &EndpointError) {
        self.record_status = "error".to_string();
        self.record_error = Some(engine.endpoint_error_to_trace(err));
        self.last_error_message = Some(err.message.clone());
    }
}

pub(super) async fn prepare_request_input(
    engine: &EndpointEngine,
    method: &Method,
    path: &str,
    parts: &Parts,
    body: Body,
    endpoint_match: &EndpointMatch<'_>,
    base_context: &JsonValue,
) -> Result<PreparedRequestInput> {
    let mut prepared = PreparedRequestInput::new();
    let body_value = match read_request_body(
        method,
        path,
        &parts.headers,
        body,
        engine.config.max_body_bytes,
    )
    .await
    {
        Ok(body) => {
            prepared.multipart_temp_dir = body.multipart_temp_dir;
            Ok(body.value)
        }
        Err(err) => Err(err),
    };

    let endpoint = endpoint_match.endpoint;
    let (record_input, current) = match body_value {
        Ok(body_value) => match build_input(parts, &endpoint_match.params, body_value.clone()) {
            Ok(input) => {
                let record_input = input.clone();
                let current_result: Result<JsonValue, EndpointError> =
                    if let Some(mappings) = &endpoint.input {
                        apply_mappings_via_rule(mappings, &input, Some(base_context))
                            .map_err(EndpointError::from_transform)
                            .map(|value| value.unwrap_or_else(empty_object))
                    } else {
                        Ok(input.clone())
                    };
                match current_result {
                    Ok(current) => Ok((record_input, current)),
                    Err(err) => handle_input_error(
                        engine,
                        &mut prepared,
                        parts,
                        endpoint_match,
                        base_context,
                        err,
                        Some(input),
                        body_value,
                    ),
                }
            }
            Err(err) => handle_input_error(
                engine,
                &mut prepared,
                parts,
                endpoint_match,
                base_context,
                err,
                None,
                body_value,
            ),
        },
        Err(err) => handle_input_error(
            engine,
            &mut prepared,
            parts,
            endpoint_match,
            base_context,
            err,
            None,
            None,
        ),
    }?;

    prepared.set_result(record_input, current);
    Ok(prepared)
}

fn handle_input_error(
    engine: &EndpointEngine,
    prepared: &mut PreparedRequestInput,
    parts: &Parts,
    endpoint_match: &EndpointMatch<'_>,
    base_context: &JsonValue,
    err: EndpointError,
    fallback_input: Option<JsonValue>,
    body_value: Option<JsonValue>,
) -> Result<(JsonValue, JsonValue)> {
    prepared.skip_steps = true;
    let fallback_input = fallback_input.unwrap_or_else(|| {
        let query = parse_query(parts.uri.query()).unwrap_or_else(|_| empty_object());
        build_input_from_parts(parts, &endpoint_match.params, body_value, query)
    });
    if let Some(catch) = &endpoint_match.endpoint.catch {
        if let Some(next) = engine
            .run_catch(
                catch,
                &err,
                &fallback_input,
                None,
                &engine.endpoint_rule.base_dir,
                base_context,
            )
            .map_err(|err| anyhow!(err.to_string()))?
        {
            Ok((fallback_input, next))
        } else {
            prepared.set_error(engine, &err);
            Ok((fallback_input.clone(), fallback_input))
        }
    } else {
        prepared.set_error(engine, &err);
        Ok((fallback_input.clone(), fallback_input))
    }
}
