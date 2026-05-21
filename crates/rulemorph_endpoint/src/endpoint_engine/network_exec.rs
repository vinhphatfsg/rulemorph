use std::time::Instant;

use anyhow::Result;
use rulemorph::{get_path, parse_path};
use serde_json::Value as JsonValue;

mod body;
mod internal_auth;
mod request;
mod response;

use super::config::RequestContext;
use super::error::{EndpointError, EndpointErrorKind};
use super::expr::{build_headers, eval_expr_string};
use super::network_rule::CompiledNetworkRule;
use super::{EndpointEngine, NetworkExecution, empty_object};

impl EndpointEngine {
    pub(super) async fn execute_network(
        &self,
        rule: &CompiledNetworkRule,
        input: &JsonValue,
        context: Option<&JsonValue>,
        request_context: Option<&RequestContext>,
    ) -> Result<NetworkExecution, EndpointError> {
        if rule.request.method == axum::http::Method::GET && rule.body.is_some() {
            return Err(EndpointError::invalid("GET with body is not allowed"));
        }

        let total_started = Instant::now();
        let empty_context = empty_object();
        let base_context = context.unwrap_or(&empty_context);
        let run_catch = |err: EndpointError,
                         request_us: u64,
                         body_rule_trace: Option<JsonValue>|
         -> Result<NetworkExecution, EndpointError> {
            if let Some(catch) = &rule.catch {
                if let Some(output) =
                    self.run_catch(catch, &err, input, None, &rule.base_dir, base_context)?
                {
                    return Ok(NetworkExecution {
                        output,
                        request_us,
                        total_us: total_started.elapsed().as_micros() as u64,
                        body_rule_trace,
                    });
                }
            }
            Err(err)
        };

        let url = match eval_expr_string(&rule.request.url, input, context) {
            Ok(url) => url,
            Err(err) => return run_catch(err, 0, None),
        };
        let mut network_context_override = None;
        let context_for_eval = if rule.internal_auth
            && self.config.allow_internal_auth
            && self.is_internal_target(&url)
        {
            if let Some(internal_api_key) = self.resolve_internal_api_key(request_context) {
                let value = self.context_with_internal_api_key(base_context, &internal_api_key);
                network_context_override = Some(value);
            }
            network_context_override
                .as_ref()
                .map(|value| value as &JsonValue)
        } else {
            None
        };
        let context_for_eval = context_for_eval.or(context);

        let headers = match build_headers(&rule.request.headers, input, context_for_eval) {
            Ok(headers) => headers,
            Err(err) => return run_catch(err, 0, None),
        };
        let body = match self.build_network_body(rule, input, context_for_eval) {
            Ok(body) => body,
            Err(err) => return run_catch(err, 0, None),
        };
        let body_rule_trace =
            Self::build_body_rule_trace(rule, input, context_for_eval, body.as_ref());

        let mut attempt = 0;
        loop {
            let request_started = Instant::now();
            let result = self
                .send_network_request(rule, &url, &headers, body.as_ref(), request_context)
                .await;
            let request_us = request_started.elapsed().as_micros() as u64;
            let run_catch_with_body =
                |err: EndpointError, request_us: u64| -> Result<NetworkExecution, EndpointError> {
                    run_catch(err, request_us, body_rule_trace.clone())
                };

            match result {
                Ok(value) => {
                    if let Some(select) = &rule.select {
                        let tokens = match parse_path(select) {
                            Ok(tokens) => tokens,
                            Err(_) => {
                                return run_catch_with_body(
                                    EndpointError::invalid(format!(
                                        "invalid select path: {}",
                                        select
                                    )),
                                    request_us,
                                );
                            }
                        };
                        let selected = match get_path(&value, &tokens) {
                            Some(selected) => selected,
                            None => {
                                return run_catch_with_body(
                                    EndpointError::invalid(format!(
                                        "select path not found: {}",
                                        select
                                    )),
                                    request_us,
                                );
                            }
                        };
                        return Ok(NetworkExecution {
                            output: selected.clone(),
                            request_us,
                            total_us: total_started.elapsed().as_micros() as u64,
                            body_rule_trace: body_rule_trace.clone(),
                        });
                    }
                    return Ok(NetworkExecution {
                        output: value,
                        request_us,
                        total_us: total_started.elapsed().as_micros() as u64,
                        body_rule_trace: body_rule_trace.clone(),
                    });
                }
                Err(err) => {
                    if let Some(retry) = &rule.retry {
                        if err.kind == EndpointErrorKind::Timeout
                            || err.kind == EndpointErrorKind::Network
                        {
                            if attempt < retry.max {
                                let delay = retry.delay_for(attempt);
                                attempt += 1;
                                tokio::time::sleep(delay).await;
                                continue;
                            }
                        }
                    }
                    return run_catch_with_body(err, request_us);
                }
            }
        }
    }
}
