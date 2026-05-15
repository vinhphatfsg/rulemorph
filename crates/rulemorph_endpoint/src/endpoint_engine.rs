#[cfg(test)]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
#[cfg(test)]
use axum::http::HeaderMap;
use axum::http::{Method, Request};
use axum::response::Response;
use chrono::Utc;
use http_body_util::LengthLimitError;
#[cfg(test)]
use rulemorph::Mapping;
use rulemorph::serde_guard::parse_yaml_value_strict;
use rulemorph::v2_eval::{V2EvalContext, eval_v2_condition};
#[cfg(test)]
use rulemorph::v2_parser::parse_v2_expr;
use rulemorph::{
    RuleFile, RuleFormat, parse_rule_file_with_format, transform_record_with_base_dir,
    validate_rule_file_with_source,
};
use rulemorph_trace::{TraceWriter, TraceWriterConfig};
use serde_json::{Value as JsonValue, json};
use tracing::warn;

const MULTIPART_IMPORT_MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
const MULTIPART_IMPORT_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const MULTIPART_IMPORT_MAX_ENTRIES: usize = 4096;
use uuid::Uuid;

mod catch;
mod config;
mod endpoint_rule;
mod error;
mod expr;
mod host;
mod multipart_import;
mod network_exec;
mod network_rule;
mod reply_context;
mod request_input;
mod rule_exec;
mod rule_ref;
mod ssrf_audit;
mod trace_graph;
mod validation;

use self::catch::CatchSpec;
pub use self::config::{ApiMode, EngineConfig, RequestContext};
#[cfg(test)]
use self::endpoint_rule::EndpointPath;
use self::endpoint_rule::{CompiledEndpointRule, CompiledStep, EndpointRuleFile};
use self::error::EndpointError;
#[cfg(test)]
use self::error::EndpointErrorKind;
use self::expr::apply_mappings_via_rule;
#[cfg(test)]
use self::expr::{build_headers, eval_expr_string};
#[cfg(test)]
use self::host::internal_hosts_match;
use self::multipart_import::build_multipart_import_body;
#[cfg(test)]
use self::multipart_import::{copy_zip_entry_bounded, extract_zip};
#[cfg(test)]
use self::network_rule::{CompiledNetworkRequest, NetworkRequest};
use self::network_rule::{
    CompiledNetworkRule, NetworkRuleFile, compile_network_rule, compile_retry, parse_duration,
};
use self::request_input::{
    build_input, build_input_from_parts, is_multipart_import_request, parse_query,
};
use self::rule_ref::{
    resolve_rule_path, rule_display_name, rule_ref_from_path, rule_ref_from_rule,
    safe_rule_ref_from_path,
};
#[cfg(test)]
use self::ssrf_audit::build_ssrf_audit_log;
#[cfg(test)]
use self::trace_graph::{build_mapping_ops_with_values, sum_rule_trace_duration_us};
#[cfg(test)]
use self::trace_graph::{build_network_nodes_with_timing, build_rule_nodes_from_rule};
pub use self::validation::{RulesDirError, RulesDirErrors, validate_rules_dir};
#[cfg(test)]
use std::time::Duration;

pub struct EndpointEngine {
    endpoint_rule: CompiledEndpointRule,
    raw_rule_source: JsonValue,
    config: EngineConfig,
    trace_writer: TraceWriter,
}

fn is_length_limit_error(err: &axum::Error) -> bool {
    let mut current: &(dyn std::error::Error + 'static) = err;
    if current.is::<LengthLimitError>() {
        return true;
    }
    while let Some(source) = current.source() {
        if source.is::<LengthLimitError>() {
            return true;
        }
        current = source;
    }
    false
}

struct NetworkExecution {
    output: JsonValue,
    request_us: u64,
    total_us: u64,
    body_rule_trace: Option<JsonValue>,
}

#[derive(Debug)]
struct LoadedRule {
    rule: RuleFile,
    base_dir: PathBuf,
}

impl EndpointEngine {
    pub fn load(rules_dir: PathBuf, config: EngineConfig) -> Result<Self> {
        let endpoint_path = rules_dir.join("endpoint.yaml");
        let source = std::fs::read_to_string(&endpoint_path)
            .with_context(|| format!("failed to read {}", endpoint_path.display()))?;
        let raw_source = parse_yaml_value_strict(&source)
            .map_err(|err| anyhow!(err))
            .with_context(|| format!("failed to parse {}", endpoint_path.display()))?;
        let raw_rule_source =
            serde_json::to_value(raw_source.clone()).unwrap_or_else(|_| json!({}));
        let raw: EndpointRuleFile = serde_yaml::from_value(raw_source)
            .with_context(|| format!("failed to parse {}", endpoint_path.display()))?;
        if raw.version != 2 {
            return Err(anyhow!("endpoint rule version must be 2"));
        }
        if raw.rule_type != "endpoint" {
            return Err(anyhow!("endpoint rule type must be endpoint"));
        }
        let compiled = CompiledEndpointRule::compile(raw.clone(), &endpoint_path)?;
        let trace_writer = TraceWriter::with_config(
            config.data_dir.clone(),
            TraceWriterConfig {
                write_options: config.trace_write_options.clone(),
                ..TraceWriterConfig::default()
            },
        );
        Ok(Self {
            endpoint_rule: compiled,
            raw_rule_source,
            config,
            trace_writer,
        })
    }

    pub fn allows_internal_auth(&self) -> bool {
        self.config.allow_internal_auth
    }

    pub fn has_endpoint(&self, method: &Method, path: &str) -> bool {
        self.endpoint_rule.match_endpoint(method, path).is_some()
    }

    pub async fn handle_request(&self, request: Request<axum::body::Body>) -> Result<Response> {
        let started = Instant::now();
        let (parts, body) = request.into_parts();
        let request_context = parts
            .extensions
            .get::<RequestContext>()
            .cloned()
            .unwrap_or_default();
        let base_context = self.build_context_json(Some(&request_context));
        let method = parts.method.clone();
        let path = parts.uri.path().to_string();
        let endpoint_match = self
            .endpoint_rule
            .match_endpoint(&method, &path)
            .ok_or_else(|| anyhow!("no endpoint matched"))?;
        let is_multipart = is_multipart_import_request(&method, &path, &parts.headers);
        let mut _multipart_temp_dir: Option<tempfile::TempDir> = None;
        let body_value = if is_multipart {
            match build_multipart_import_body(&parts.headers, body).await {
                Ok((body, temp_dir)) => {
                    _multipart_temp_dir = Some(temp_dir);
                    Ok(Some(body))
                }
                Err(err) => Err(err),
            }
        } else {
            let body_limit = self.config.max_body_bytes;
            match axum::body::to_bytes(body, body_limit).await {
                Ok(body_bytes) if body_bytes.is_empty() => Ok(None),
                Ok(body_bytes) => serde_json::from_slice::<JsonValue>(&body_bytes)
                    .map(Some)
                    .map_err(|err| EndpointError::invalid(err.to_string())),
                Err(err) if is_length_limit_error(&err) => {
                    Err(EndpointError::payload_too_large(body_limit))
                }
                Err(err) => Err(EndpointError::network(format!(
                    "request body read error: {}",
                    err
                ))),
            }
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

    fn build_trace(
        &self,
        method: &Method,
        path: &str,
        input: JsonValue,
        output: JsonValue,
        status: String,
        error: Option<JsonValue>,
        nodes: Vec<JsonValue>,
        duration_us: u64,
    ) -> JsonValue {
        let trace_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let rule_path = rule_ref_from_path(
            &self.endpoint_rule.base_dir,
            &self.endpoint_rule.source_path,
        );
        let rule_source = self.raw_rule_source.clone();
        let record = json!({
            "index": 0,
            "status": status,
            "duration_us": duration_us,
            "input": input,
            "output": output,
            "nodes": nodes,
            "error": error
        });
        json!({
            "trace_id": trace_id,
            "status": status,
            "timestamp": now.to_rfc3339(),
            "rule": {
                "type": "endpoint",
                "name": format!("{} {}", method.as_str(), path),
                "path": rule_path,
                "version": 2
            },
            "input_format": "json",
            "rule_source": rule_source,
            "records": [record],
            "summary": {
                "record_total": 1,
                "record_success": if status == "ok" { 1 } else { 0 },
                "record_failed": if status == "ok" { 0 } else { 1 },
                "duration_us": duration_us
            }
        })
    }

    fn build_step_trace(
        &self,
        step_index: usize,
        step: &CompiledStep,
        status: &str,
        input: JsonValue,
        output: Option<JsonValue>,
        error: Option<EndpointError>,
        duration_us: u64,
        child_trace: Option<JsonValue>,
    ) -> JsonValue {
        let label = step_label(&step.rule);
        let rule_ref = rule_ref_from_rule(&self.endpoint_rule.base_dir, &step.rule);
        let mut node = json!({
            "id": format!("step-{}", step_index),
            "kind": "endpoint",
            "label": label,
            "status": status,
            "input": input,
            "output": output,
            "duration_us": duration_us,
            "meta": {
                "rule_ref": rule_ref,
                "step_index": step_index
            }
        });
        if let Some(err) = error {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("error".to_string(), self.endpoint_error_to_trace(&err));
            }
        }
        if let Some(child_trace) = child_trace {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("child_trace".to_string(), child_trace);
            }
        }
        node
    }

    fn endpoint_error_to_trace(&self, err: &EndpointError) -> JsonValue {
        let path = err
            .path
            .as_ref()
            .and_then(|path| safe_rule_ref_from_path(&self.endpoint_rule.base_dir, path));
        json!({
            "code": format!("{:?}", err.kind),
            "message": err.message,
            "path": path
        })
    }

    async fn write_trace(&self, trace: JsonValue) -> Result<()> {
        let trace_id = trace
            .get("trace_id")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string();
        if !self.trace_writer.enqueue(trace) {
            warn!("trace queue full; dropped trace {}", trace_id);
        }
        Ok(())
    }

    fn run_catch(
        &self,
        catch: &CatchSpec,
        error: &EndpointError,
        input: &JsonValue,
        params: Option<&JsonValue>,
        base_dir: &Path,
        base_context: &JsonValue,
    ) -> Result<Option<JsonValue>, EndpointError> {
        if let Some(target) = catch.match_target(error) {
            let target_path = resolve_rule_path(base_dir, &target.to_string_lossy());
            let rule = match load_rule_kind(&target_path)
                .map_err(|err| EndpointError::invalid(err.to_string()))?
            {
                RuleKind::Normal(rule) => rule,
                RuleKind::Network(_) => {
                    return Err(EndpointError::invalid("catch rule must be normal"));
                }
            };
            let error_context = self.step_context(base_context, params, Some(error));
            let output = transform_record_with_base_dir(
                &rule.rule,
                input,
                Some(&error_context),
                &rule.base_dir,
            )
            .map_err(EndpointError::from_transform)?
            .unwrap_or_else(empty_object);
            return Ok(Some(output));
        }
        Ok(None)
    }
}

fn load_rule_kind(path: &Path) -> Result<RuleKind> {
    let source = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let meta = parse_yaml_value_strict(&source)
        .map_err(|err| anyhow!(err))
        .with_context(|| format!("failed to parse {}", path.display()))?;
    let rule_type = meta
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("normal");
    match rule_type {
        "network" => {
            let raw: NetworkRuleFile = serde_yaml::from_value(meta)
                .with_context(|| format!("failed to parse {}", path.display()))?;
            let compiled = compile_network_rule(raw, path)?;
            Ok(RuleKind::Network(compiled))
        }
        "endpoint" => Err(anyhow!("endpoint rule not allowed as step")),
        _ => {
            let rule = parse_rule_file_with_format(&source, RuleFormat::from_path(path))
                .with_context(|| format!("failed to parse {}", path.display()))?;
            validate_rule_file_with_source(&rule, &source)
                .map_err(|err| anyhow!("failed to validate {}: {:?}", path.display(), err))?;
            let base_dir = path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf();
            Ok(RuleKind::Normal(LoadedRule { rule, base_dir }))
        }
    }
}

fn empty_object() -> JsonValue {
    JsonValue::Object(serde_json::Map::new())
}

fn step_label(rule: &str) -> String {
    let path = Path::new(rule);
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(rule)
        .to_string()
}

fn yaml_source_to_json(source: &str) -> Option<JsonValue> {
    let raw = parse_yaml_value_strict(source).ok()?;
    serde_json::to_value(raw).ok()
}

enum RuleKind {
    Normal(LoadedRule),
    Network(CompiledNetworkRule),
}

#[cfg(test)]
mod tests;
