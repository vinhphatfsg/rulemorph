use std::time::Instant;

use anyhow::{Result, anyhow};
use axum::http::Request;
use axum::response::Response;
use tracing::warn;

use super::EndpointEngine;
use super::error::EndpointError;

mod input;
mod steps;

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

        let endpoint = endpoint_match.endpoint;
        let prepared_input = input::prepare_request_input(
            self,
            &method,
            &path,
            &parts,
            body,
            &endpoint_match,
            &base_context,
        )
        .await?;
        let mut _multipart_temp_dir = prepared_input.multipart_temp_dir;
        let record_input = prepared_input.record_input;
        let step_run = self
            .run_endpoint_steps(
                endpoint,
                prepared_input.current,
                prepared_input.record_status,
                prepared_input.record_error,
                prepared_input.last_error_message,
                prepared_input.skip_steps,
                &base_context,
                &request_context,
            )
            .await?;
        let mut current = step_run.current;
        let nodes = step_run.nodes;
        let mut record_status = step_run.record_status;
        let mut record_error = step_run.record_error;
        let last_error_message = step_run.last_error_message;

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
