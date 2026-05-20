use std::time::Instant;

use anyhow::{Result, anyhow};
use axum::http::Request;
use axum::response::Response;
use rulemorph::v2_eval::{V2EvalContext, eval_v2_condition};
use serde_json::Value as JsonValue;
use tracing::warn;

use super::error::EndpointError;
use super::expr::apply_mappings_via_rule;
use super::request_body::read_request_body;
use super::request_input::{build_input, build_input_from_parts, parse_query};
use super::{EndpointEngine, empty_object};

impl EndpointEngine {
    pub async fn handle_request(&self, request: Request<axum::body::Body>) -> Result<Response> {
        let started = Instant::now();
        let (parts, body) = request.into_parts();
        let request_context = parts
            .extensions
            .get::<super::RequestContext>()
            .cloned()
            .unwrap_or_default();
        let base_context = self.build_context_json(Some(&request_context));
        let method = parts.method.clone();
        let path = parts.uri.path().to_string();
        let endpoint_match = self
            .endpoint_rule
            .match_endpoint(&method, &path)
            .ok_or_else(|| anyhow!("no endpoint matched"))?;
        let mut _multipart_temp_dir: Option<tempfile::TempDir> = None;
        let body_value = match read_request_body(
            &method,
            &path,
            &parts.headers,
            body,
            self.config.max_body_bytes,
        )
        .await
        {
            Ok(body) => {
                _multipart_temp_dir = body.multipart_temp_dir;
                Ok(body.value)
            }
            Err(err) => Err(err),
        };

        let endpoint = endpoint_match.endpoint;
        let mut nodes: Vec<JsonValue> = Vec::new();
        let mut record_status = "ok".to_string();
        let mut record_error: Option<JsonValue> = None;
        let mut last_error_message: Option<String> = None;
        let mut skip_steps = false;

        let mut handle_input_error = |err: EndpointError,
                                      fallback_input: Option<JsonValue>,
                                      body_value: Option<JsonValue>|
         -> Result<(JsonValue, JsonValue)> {
            skip_steps = true;
            let fallback_input = fallback_input.unwrap_or_else(|| {
                let query = parse_query(parts.uri.query()).unwrap_or_else(|_| empty_object());
                build_input_from_parts(&parts, &endpoint_match.params, body_value, query)
            });
            if let Some(catch) = &endpoint.catch {
                if let Some(next) = self
                    .run_catch(
                        catch,
                        &err,
                        &fallback_input,
                        None,
                        &self.endpoint_rule.base_dir,
                        &base_context,
                    )
                    .map_err(|err| anyhow!(err.to_string()))?
                {
                    Ok((fallback_input, next))
                } else {
                    record_status = "error".to_string();
                    record_error = Some(self.endpoint_error_to_trace(&err));
                    last_error_message = Some(err.message.clone());
                    Ok((fallback_input.clone(), fallback_input))
                }
            } else {
                record_status = "error".to_string();
                record_error = Some(self.endpoint_error_to_trace(&err));
                last_error_message = Some(err.message.clone());
                Ok((fallback_input.clone(), fallback_input))
            }
        };

        let (record_input, mut current) = match body_value {
            Ok(body_value) => match build_input(&parts, &endpoint_match.params, body_value.clone())
            {
                Ok(input) => {
                    let record_input = input.clone();
                    let current_result: Result<JsonValue, EndpointError> =
                        if let Some(mappings) = &endpoint.input {
                            apply_mappings_via_rule(mappings, &input, Some(&base_context))
                                .map_err(EndpointError::from_transform)
                                .map(|value| value.unwrap_or_else(empty_object))
                        } else {
                            Ok(input.clone())
                        };
                    match current_result {
                        Ok(current) => Ok((record_input, current)),
                        Err(err) => handle_input_error(err, Some(input), body_value),
                    }
                }
                Err(err) => handle_input_error(err, None, body_value),
            },
            Err(err) => handle_input_error(err, None, None),
        }?;

        if !skip_steps {
            for (step_index, step) in endpoint.steps.iter().enumerate() {
                let step_input = current.clone();
                let step_started = Instant::now();
                if let Some(condition) = &step.when {
                    let ctx = V2EvalContext::new();
                    let keep = eval_v2_condition(
                        condition,
                        &current,
                        Some(&base_context),
                        &empty_object(),
                        "steps.when",
                        &ctx,
                    )?;
                    if !keep {
                        let duration_us = step_started.elapsed().as_micros() as u64;
                        nodes.push(self.build_step_trace(
                            step_index,
                            step,
                            "skipped",
                            step_input,
                            Some(current.clone()),
                            None,
                            duration_us,
                            None,
                        ));
                        continue;
                    }
                }
                let step_context = self.step_context(&base_context, step.with.as_ref(), None);
                let step_result = self
                    .execute_rule(
                        &step.rule,
                        &current,
                        Some(&step_context),
                        &self.endpoint_rule.base_dir,
                        Some(&request_context),
                    )
                    .await;
                match step_result {
                    Ok(execution) => {
                        current = execution.output.clone();
                        let duration_us = step_started.elapsed().as_micros() as u64;
                        nodes.push(self.build_step_trace(
                            step_index,
                            step,
                            "ok",
                            step_input,
                            Some(execution.output),
                            None,
                            duration_us,
                            execution.child_trace,
                        ));
                    }
                    Err(err) => {
                        if let Some(catch) = &step.catch {
                            if let Some(next) = self
                                .run_catch(
                                    catch,
                                    &err.error,
                                    &current,
                                    step.with.as_ref(),
                                    &self.endpoint_rule.base_dir,
                                    &base_context,
                                )
                                .map_err(|err| anyhow!(err.to_string()))?
                            {
                                current = next.clone();
                                let duration_us = step_started.elapsed().as_micros() as u64;
                                nodes.push(self.build_step_trace(
                                    step_index,
                                    step,
                                    "ok",
                                    step_input,
                                    Some(next),
                                    None,
                                    duration_us,
                                    None,
                                ));
                                continue;
                            }
                        }

                        if let Some(catch) = &endpoint.catch {
                            if let Some(next) = self
                                .run_catch(
                                    catch,
                                    &err.error,
                                    &current,
                                    None,
                                    &self.endpoint_rule.base_dir,
                                    &base_context,
                                )
                                .map_err(|err| anyhow!(err.to_string()))?
                            {
                                current = next.clone();
                                let duration_us = step_started.elapsed().as_micros() as u64;
                                nodes.push(self.build_step_trace(
                                    step_index,
                                    step,
                                    "ok",
                                    step_input,
                                    Some(next),
                                    None,
                                    duration_us,
                                    None,
                                ));
                                break;
                            }
                        }

                        record_status = "error".to_string();
                        record_error = Some(self.endpoint_error_to_trace(&err.error));
                        last_error_message = Some(err.error.message.clone());
                        let duration_us = step_started.elapsed().as_micros() as u64;
                        nodes.push(self.build_step_trace(
                            step_index,
                            step,
                            "error",
                            step_input,
                            None,
                            Some(err.error.clone()),
                            duration_us,
                            err.child_trace,
                        ));
                        break;
                    }
                }
            }
        }

        let response_result = if record_status == "error" {
            Err(anyhow!(
                last_error_message.unwrap_or_else(|| "endpoint error".to_string())
            ))
        } else {
            match self.build_reply(&endpoint.reply, &current, &base_context) {
                Ok(response) => Ok(response),
                Err(err) => {
                    let reply_error = EndpointError::invalid(err.to_string());
                    let catch_output = if let Some(catch) = &endpoint.catch {
                        self.run_catch(
                            catch,
                            &reply_error,
                            &current,
                            None,
                            &self.endpoint_rule.base_dir,
                            &base_context,
                        )
                        .map_err(|err| anyhow!(err.to_string()))?
                    } else {
                        None
                    };

                    if let Some(next) = catch_output {
                        current = next;
                        match self.build_reply(&endpoint.reply, &current, &base_context) {
                            Ok(response) => Ok(response),
                            Err(err) => {
                                let reply_error = EndpointError::invalid(err.to_string());
                                record_status = "error".to_string();
                                record_error = Some(self.endpoint_error_to_trace(&reply_error));
                                Err(anyhow!(reply_error.message))
                            }
                        }
                    } else {
                        record_status = "error".to_string();
                        record_error = Some(self.endpoint_error_to_trace(&reply_error));
                        Err(anyhow!(reply_error.message))
                    }
                }
            }
        };

        let duration_us = started.elapsed().as_micros() as u64;
        let trace = self.build_trace(
            &method,
            &path,
            record_input,
            current.clone(),
            record_status,
            record_error,
            nodes,
            duration_us,
        );
        if let Err(err) = self.write_trace(trace).await {
            warn!("failed to write trace: {}", err);
        }

        response_result
    }
}
