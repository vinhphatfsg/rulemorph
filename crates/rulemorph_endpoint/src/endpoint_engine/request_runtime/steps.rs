use std::time::Instant;

use anyhow::{Result, anyhow};
use rulemorph::v2_eval::{V2EvalContext, eval_v2_condition};
use serde_json::Value as JsonValue;

use super::super::config::RequestContext;
use super::super::endpoint_rule::CompiledEndpoint;
use super::super::trace_emit::EndpointStepTraceInput;
use super::super::{EndpointEngine, empty_object};

pub(super) struct EndpointStepRun {
    pub(super) current: JsonValue,
    pub(super) nodes: Vec<JsonValue>,
    pub(super) record_status: String,
    pub(super) record_error: Option<JsonValue>,
    pub(super) last_error_message: Option<String>,
}

pub(super) struct EndpointStepRunInput<'a> {
    pub(super) endpoint: &'a CompiledEndpoint,
    pub(super) current: JsonValue,
    pub(super) record_status: String,
    pub(super) record_error: Option<JsonValue>,
    pub(super) last_error_message: Option<String>,
    pub(super) skip_steps: bool,
    pub(super) base_context: &'a JsonValue,
    pub(super) request_context: &'a RequestContext,
}

impl EndpointEngine {
    pub(super) async fn run_endpoint_steps(
        &self,
        input: EndpointStepRunInput<'_>,
    ) -> Result<EndpointStepRun> {
        let EndpointStepRunInput {
            endpoint,
            mut current,
            mut record_status,
            mut record_error,
            mut last_error_message,
            skip_steps,
            base_context,
            request_context,
        } = input;
        let mut nodes: Vec<JsonValue> = Vec::new();
        if !skip_steps {
            for (step_index, step) in endpoint.steps.iter().enumerate() {
                let step_input = current.clone();
                let step_started = Instant::now();
                if let Some(condition) = &step.when {
                    let ctx = V2EvalContext::new();
                    let keep = eval_v2_condition(
                        condition,
                        &current,
                        Some(base_context),
                        &empty_object(),
                        "steps.when",
                        &ctx,
                    )?;
                    if !keep {
                        let duration_us = step_started.elapsed().as_micros() as u64;
                        nodes.push(self.build_step_trace(EndpointStepTraceInput {
                            step_index,
                            step,
                            status: "skipped",
                            input: step_input,
                            output: Some(current.clone()),
                            error: None,
                            duration_us,
                            child_trace: None,
                        }));
                        continue;
                    }
                }
                let step_context = self.step_context(base_context, step.with.as_ref(), None);
                let step_result = self
                    .execute_rule(
                        &step.rule,
                        &current,
                        Some(&step_context),
                        &self.endpoint_rule.base_dir,
                        Some(request_context),
                    )
                    .await;
                match step_result {
                    Ok(execution) => {
                        current = execution.output.clone();
                        let duration_us = step_started.elapsed().as_micros() as u64;
                        nodes.push(self.build_step_trace(EndpointStepTraceInput {
                            step_index,
                            step,
                            status: "ok",
                            input: step_input,
                            output: Some(execution.output),
                            error: None,
                            duration_us,
                            child_trace: execution.child_trace,
                        }));
                    }
                    Err(err) => {
                        if let Some(catch) = &step.catch
                            && let Some(next) = self
                                .run_catch(
                                    catch,
                                    &err.error,
                                    &current,
                                    step.with.as_ref(),
                                    &self.endpoint_rule.base_dir,
                                    base_context,
                                )
                                .map_err(|err| anyhow!(err.to_string()))?
                        {
                            current = next.clone();
                            let duration_us = step_started.elapsed().as_micros() as u64;
                            nodes.push(self.build_step_trace(EndpointStepTraceInput {
                                step_index,
                                step,
                                status: "ok",
                                input: step_input,
                                output: Some(next),
                                error: None,
                                duration_us,
                                child_trace: None,
                            }));
                            continue;
                        }

                        if let Some(catch) = &endpoint.catch
                            && let Some(next) = self
                                .run_catch(
                                    catch,
                                    &err.error,
                                    &current,
                                    None,
                                    &self.endpoint_rule.base_dir,
                                    base_context,
                                )
                                .map_err(|err| anyhow!(err.to_string()))?
                        {
                            current = next.clone();
                            let duration_us = step_started.elapsed().as_micros() as u64;
                            nodes.push(self.build_step_trace(EndpointStepTraceInput {
                                step_index,
                                step,
                                status: "ok",
                                input: step_input,
                                output: Some(next),
                                error: None,
                                duration_us,
                                child_trace: None,
                            }));
                            break;
                        }

                        record_status = "error".to_string();
                        record_error = Some(self.endpoint_error_to_trace(&err.error));
                        last_error_message = Some(err.error.message.clone());
                        let duration_us = step_started.elapsed().as_micros() as u64;
                        nodes.push(self.build_step_trace(EndpointStepTraceInput {
                            step_index,
                            step,
                            status: "error",
                            input: step_input,
                            output: None,
                            error: Some(err.error.clone()),
                            duration_us,
                            child_trace: err.child_trace,
                        }));
                        break;
                    }
                }
            }
        }

        Ok(EndpointStepRun {
            current,
            nodes,
            record_status,
            record_error,
            last_error_message,
        })
    }
}
