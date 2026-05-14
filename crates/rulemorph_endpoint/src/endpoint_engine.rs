use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode};
use axum::response::Response;
use chrono::Utc;
use http_body_util::LengthLimitError;
use reqwest::Client;
use rulemorph::serde_guard::parse_yaml_value_strict;
use rulemorph::v2_eval::{EvalValue, V2EvalContext, eval_v2_condition, eval_v2_expr};
use rulemorph::v2_parser::{parse_v2_condition, parse_v2_expr};
use rulemorph::{
    Mapping, RuleFile, RuleFormat, TransformError, get_path, parse_path,
    parse_rule_file_with_format, transform_record, transform_record_with_base_dir,
    validate_rule_file_with_source,
};
use rulemorph_trace::{TraceWriteOptions, TraceWriter, TraceWriterConfig};
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tracing::warn;

const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;
const MULTIPART_IMPORT_MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
const MULTIPART_IMPORT_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const MULTIPART_IMPORT_MAX_ENTRIES: usize = 4096;
use uuid::Uuid;

use crate::ssrf::{ResolvedSsrfTarget, resolve_ssrf_target};

mod error;
mod multipart_import;
mod trace_graph;
mod validation;

use self::error::{EndpointError, EndpointErrorKind};
use self::multipart_import::build_multipart_import_body;
#[cfg(test)]
use self::multipart_import::{copy_zip_entry_bounded, extract_zip};
#[cfg(test)]
use self::trace_graph::{build_mapping_ops_with_values, sum_rule_trace_duration_us};
use self::trace_graph::{
    build_network_nodes_with_timing, build_rule_nodes_from_rule, build_rule_trace,
};
pub use self::validation::{RulesDirError, RulesDirErrors, validate_rules_dir};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiMode {
    UiOnly,
    Rules,
}

#[derive(Clone, Debug, Default)]
pub struct RequestContext {
    pub tenant_id: Option<String>,
    pub internal_api_key: Option<String>,
}

impl Default for ApiMode {
    fn default() -> Self {
        ApiMode::Rules
    }
}

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub internal_base: String,
    pub data_dir: PathBuf,
    pub trace_write_options: TraceWriteOptions,
    pub max_body_bytes: usize,
    pub max_response_bytes: usize,
    pub ssrf_allowlist: Vec<String>,
    pub ssrf_allow_private: bool,
    pub ssrf_private_allowlist: Vec<String>,
    pub allow_internal_auth: bool,
    pub internal_auth_path_allowlist: Vec<String>,
    pub internal_api_key: Option<String>,
}

impl EngineConfig {
    pub fn new(internal_base: String, data_dir: PathBuf) -> Self {
        let mut config = Self {
            internal_base,
            data_dir,
            trace_write_options: TraceWriteOptions::default(),
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            ssrf_allowlist: Vec::new(),
            ssrf_allow_private: false,
            ssrf_private_allowlist: Vec::new(),
            allow_internal_auth: false,
            internal_auth_path_allowlist: Vec::new(),
            internal_api_key: None,
        };
        if let Ok(parsed) = url::Url::parse(&config.internal_base) {
            if let Some(host) = parsed.host_str() {
                config.ssrf_private_allowlist.push(host.to_string());
                if is_loopback_host(&normalize_internal_host(host)) {
                    config
                        .ssrf_private_allowlist
                        .extend(["localhost", "127.0.0.1", "::1"].map(str::to_string));
                    config.ssrf_private_allowlist.sort();
                    config.ssrf_private_allowlist.dedup();
                }
            }
        }
        config
    }

    pub fn with_trace_write_options(mut self, trace_write_options: TraceWriteOptions) -> Self {
        self.trace_write_options = trace_write_options;
        self
    }

    pub fn with_max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.max_body_bytes = max_body_bytes;
        self
    }

    pub fn with_max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = max_response_bytes;
        self
    }

    pub fn with_ssrf_allowlist(mut self, ssrf_allowlist: Vec<String>) -> Self {
        self.ssrf_allowlist = ssrf_allowlist;
        self
    }

    pub fn with_ssrf_allow_private(mut self, ssrf_allow_private: bool) -> Self {
        self.ssrf_allow_private = ssrf_allow_private;
        self
    }

    pub fn with_ssrf_private_allowlist(mut self, ssrf_private_allowlist: Vec<String>) -> Self {
        self.ssrf_private_allowlist = ssrf_private_allowlist;
        self
    }

    pub fn with_internal_auth_enabled(mut self, enabled: bool) -> Self {
        self.allow_internal_auth = enabled;
        self
    }

    pub fn with_internal_auth_path_allowlist(mut self, allowlist: Vec<String>) -> Self {
        self.internal_auth_path_allowlist = allowlist;
        self
    }

    pub fn with_internal_api_key(mut self, internal_api_key: String) -> Self {
        self.internal_api_key = Some(internal_api_key);
        self.allow_internal_auth = true;
        self
    }
}

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

struct RuleExecution {
    output: JsonValue,
    child_trace: Option<JsonValue>,
}

struct RuleExecutionError {
    error: EndpointError,
    child_trace: Option<JsonValue>,
}

impl RuleExecutionError {
    fn new(error: EndpointError) -> Self {
        Self {
            error,
            child_trace: None,
        }
    }

    fn with_child_trace(mut self, trace: Option<JsonValue>) -> Self {
        self.child_trace = trace;
        self
    }
}

impl From<EndpointError> for RuleExecutionError {
    fn from(error: EndpointError) -> Self {
        Self::new(error)
    }
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

    async fn execute_rule(
        &self,
        rule_path: &str,
        input: &JsonValue,
        context: Option<&JsonValue>,
        base_dir: &Path,
        request_context: Option<&RequestContext>,
    ) -> Result<RuleExecution, RuleExecutionError> {
        let resolved = resolve_rule_path(base_dir, rule_path);
        let rule_source = std::fs::read_to_string(&resolved)
            .ok()
            .and_then(|source| yaml_source_to_json(&source))
            .unwrap_or_else(|| json!({}));
        let rule_ref = rule_ref_from_path(base_dir, &resolved);
        match load_rule_kind(&resolved).map_err(|err| {
            RuleExecutionError::new(
                EndpointError::invalid(err.to_string()).with_path(resolved.clone()),
            )
        })? {
            RuleKind::Normal(rule) => {
                let rule_trace =
                    build_rule_nodes_from_rule(&rule.rule, input, context, &rule.base_dir);
                let duration_us = rule_trace.duration_us;
                let output_result =
                    transform_record_with_base_dir(&rule.rule, input, context, &rule.base_dir);
                let output = match output_result {
                    Ok(Some(output)) => output,
                    Ok(None) => {
                        let record_output = rule_trace
                            .pre_finalize_output
                            .clone()
                            .unwrap_or(JsonValue::Null);
                        let child_trace = build_rule_trace(
                            "normal",
                            rule_display_name(&resolved),
                            rule_ref,
                            rule.rule.version,
                            rule_source,
                            input.clone(),
                            record_output,
                            rule_trace.nodes,
                            rule_trace.finalize,
                            duration_us,
                            "error",
                        );
                        return Err(RuleExecutionError::new(
                            EndpointError::invalid(format!(
                                "record excluded by rule: {}",
                                rule_display_name(&resolved)
                            ))
                            .with_path(resolved.clone()),
                        )
                        .with_child_trace(Some(child_trace)));
                    }
                    Err(err) => {
                        let record_output = rule_trace
                            .pre_finalize_output
                            .clone()
                            .unwrap_or(JsonValue::Null);
                        let child_trace = build_rule_trace(
                            "normal",
                            rule_display_name(&resolved),
                            rule_ref,
                            rule.rule.version,
                            rule_source,
                            input.clone(),
                            record_output,
                            rule_trace.nodes,
                            rule_trace.finalize,
                            duration_us,
                            "error",
                        );
                        return Err(RuleExecutionError::new(
                            EndpointError::from_transform(err).with_path(resolved.clone()),
                        )
                        .with_child_trace(Some(child_trace)));
                    }
                };
                let record_output = rule_trace
                    .pre_finalize_output
                    .clone()
                    .unwrap_or_else(|| output.clone());
                let child_trace = build_rule_trace(
                    "normal",
                    rule_display_name(&resolved),
                    rule_ref,
                    rule.rule.version,
                    rule_source,
                    input.clone(),
                    record_output,
                    rule_trace.nodes,
                    rule_trace.finalize,
                    duration_us,
                    "ok",
                );
                Ok(RuleExecution {
                    output,
                    child_trace: Some(child_trace),
                })
            }
            RuleKind::Network(rule) => {
                let execution = self
                    .execute_network(&rule, input, context, request_context)
                    .await
                    .map_err(|err| RuleExecutionError::new(err.with_path(resolved.clone())))?;
                let nodes = build_network_nodes_with_timing(&rule, &execution);
                let child_trace = build_rule_trace(
                    "network",
                    rule_display_name(&resolved),
                    rule_ref,
                    2,
                    rule_source,
                    input.clone(),
                    execution.output.clone(),
                    nodes,
                    None,
                    execution.total_us,
                    "ok",
                );
                Ok(RuleExecution {
                    output: execution.output,
                    child_trace: Some(child_trace),
                })
            }
        }
    }

    async fn execute_network(
        &self,
        rule: &CompiledNetworkRule,
        input: &JsonValue,
        context: Option<&JsonValue>,
        request_context: Option<&RequestContext>,
    ) -> Result<NetworkExecution, EndpointError> {
        if rule.request.method == Method::GET && rule.body.is_some() {
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

    fn build_network_body(
        &self,
        rule: &CompiledNetworkRule,
        input: &JsonValue,
        context: Option<&JsonValue>,
    ) -> Result<Option<JsonValue>, EndpointError> {
        if let Some(body_expr) = &rule.body {
            let value = eval_expr_value(body_expr, input, context)
                .map_err(|err| EndpointError::invalid(err.to_string()))?;
            return Ok(match value {
                EvalValue::Missing => None,
                EvalValue::Value(val) => Some(val),
            });
        }
        if let Some(mappings) = &rule.body_map {
            let output = apply_mappings_via_rule(mappings, input, context)
                .map_err(EndpointError::from_transform)?
                .unwrap_or_else(empty_object);
            return Ok(Some(output));
        }
        if let Some(body_rule) = &rule.body_rule {
            let output = transform_record_with_base_dir(
                &body_rule.rule,
                input,
                context,
                &body_rule.base_dir,
            )
            .map_err(EndpointError::from_transform)?;
            return Ok(output);
        }
        Ok(None)
    }

    fn build_body_rule_trace(
        rule: &CompiledNetworkRule,
        input: &JsonValue,
        context: Option<&JsonValue>,
        output: Option<&JsonValue>,
    ) -> Option<JsonValue> {
        let body_rule = rule.body_rule.as_ref()?;
        let rule_ref = rule
            .body_rule_ref
            .clone()
            .unwrap_or_else(|| "body_rule".to_string());
        let name = Path::new(&rule_ref)
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("body_rule")
            .to_string();
        let rule_trace =
            build_rule_nodes_from_rule(&body_rule.rule, input, context, &body_rule.base_dir);
        let duration_us = rule_trace.duration_us;
        let output_value = rule_trace
            .pre_finalize_output
            .clone()
            .or_else(|| output.cloned())
            .unwrap_or(JsonValue::Null);
        Some(build_rule_trace(
            "normal",
            name,
            rule_ref,
            body_rule.rule.version,
            json!({}),
            input.clone(),
            output_value,
            rule_trace.nodes,
            rule_trace.finalize,
            duration_us,
            "ok",
        ))
    }

    fn build_resolved_client(&self, target: &ResolvedSsrfTarget) -> Result<Client, EndpointError> {
        Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .resolve(&target.host, target.addr)
            .build()
            .map_err(|err| EndpointError::network(err.to_string()))
    }

    fn is_internal_target(&self, url: &str) -> bool {
        let Ok(target) = url::Url::parse(url) else {
            return false;
        };
        let Ok(base) = url::Url::parse(&self.config.internal_base) else {
            return false;
        };
        target.scheme() == base.scheme()
            && internal_hosts_match(target.host_str(), base.host_str())
            && target.port_or_known_default() == base.port_or_known_default()
    }

    fn resolve_internal_api_key(&self, request_context: Option<&RequestContext>) -> Option<String> {
        if let Some(context) = request_context {
            if let Some(key) = context.internal_api_key.as_ref() {
                return Some(key.clone());
            }
        }
        self.config.internal_api_key.clone()
    }

    fn context_with_internal_api_key(
        &self,
        base_context: &JsonValue,
        internal_api_key: &str,
    ) -> JsonValue {
        let mut value = base_context.clone();
        if let JsonValue::Object(ref mut map) = value {
            let config = map
                .entry("config".to_string())
                .or_insert_with(|| JsonValue::Object(JsonMap::new()));
            if let JsonValue::Object(config) = config {
                config.insert(
                    "internal_api_key".to_string(),
                    JsonValue::String(internal_api_key.to_string()),
                );
            }
        }
        value
    }

    fn ensure_internal_auth_path_allowed(&self, url: &str) -> Result<(), EndpointError> {
        if self.config.internal_auth_path_allowlist.is_empty() {
            return Err(EndpointError::invalid(
                "internal_auth path allowlist not configured",
            ));
        }
        let parsed =
            url::Url::parse(url).map_err(|_| EndpointError::invalid("invalid internal url"))?;
        let path = parsed.path();
        let allowed = self
            .config
            .internal_auth_path_allowlist
            .iter()
            .any(|entry| {
                if entry.ends_with('/') {
                    path.starts_with(entry)
                } else {
                    path == entry
                }
            });
        if allowed {
            Ok(())
        } else {
            Err(EndpointError::invalid("internal_auth path not allowed"))
        }
    }

    async fn send_network_request(
        &self,
        rule: &CompiledNetworkRule,
        url: &str,
        headers: &HeaderMap,
        body: Option<&JsonValue>,
        request_context: Option<&RequestContext>,
    ) -> Result<JsonValue, EndpointError> {
        if rule.internal_auth && !self.config.allow_internal_auth {
            return Err(EndpointError::invalid("internal_auth is not allowed"));
        }
        let value = tokio::time::timeout(rule.timeout, async {
            let internal_auth_allowed = rule.internal_auth && self.config.allow_internal_auth;
            let is_internal = internal_auth_allowed && self.is_internal_target(url);
            if rule.internal_auth && self.config.allow_internal_auth && !is_internal {
                return Err(EndpointError::invalid(
                    "internal_auth requires internal_base",
                ));
            }
            let allow_private_hosts: &[String] = if is_internal {
                &self.config.ssrf_private_allowlist
            } else {
                &[]
            };
            if is_internal {
                self.ensure_internal_auth_path_allowed(url)?;
            }
            let target = match resolve_ssrf_target(
                url,
                &self.config.ssrf_allowlist,
                self.config.ssrf_allow_private,
                allow_private_hosts,
            )
            .await
            {
                Ok(target) => target,
                Err(reason) => {
                    self.log_ssrf_block(rule, url, &reason, request_context);
                    return Err(EndpointError::invalid(reason));
                }
            };
            let client = self.build_resolved_client(&target)?;
            let mut req = client.request(rule.request.method.clone(), url);
            let mut headers = headers.clone();
            if is_internal {
                if let Some(internal_api_key) = self.resolve_internal_api_key(request_context) {
                    if !headers.contains_key("x-api-key") {
                        let value = HeaderValue::from_str(&internal_api_key)
                            .map_err(|_| EndpointError::invalid("invalid internal api key"))?;
                        headers.insert(HeaderName::from_static("x-api-key"), value);
                    }
                }
                if let Some(tenant_id) = request_context.and_then(|ctx| ctx.tenant_id.as_ref()) {
                    let value = HeaderValue::from_str(tenant_id)
                        .map_err(|_| EndpointError::invalid("invalid tenant id"))?;
                    headers.insert(HeaderName::from_static("x-tenant-id"), value);
                }
            }
            if body.is_some() && !headers.contains_key("content-type") {
                headers.insert(
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_static("application/json"),
                );
            }
            req = req.headers(headers);
            if let Some(body) = body {
                req = req.json(body);
            }

            let mut response = req
                .send()
                .await
                .map_err(|err| EndpointError::network(err.to_string()))?;

            let status = response.status();
            let status_u16 = status.as_u16();
            if status.is_client_error() || status.is_server_error() {
                return Err(EndpointError::http_status(status_u16));
            }

            let max_response_bytes = self.config.max_response_bytes;
            if let Some(length) = response.content_length() {
                if length > max_response_bytes as u64 {
                    return Err(EndpointError::payload_too_large(max_response_bytes));
                }
            }
            let mut bytes: Vec<u8> = Vec::new();
            let mut total = 0usize;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|err| EndpointError::network(err.to_string()))?
            {
                total = total.saturating_add(chunk.len());
                if total > max_response_bytes {
                    return Err(EndpointError::payload_too_large(max_response_bytes));
                }
                bytes.extend_from_slice(&chunk);
            }
            let value = if bytes.is_empty() {
                JsonValue::Null
            } else {
                serde_json::from_slice::<JsonValue>(&bytes)
                    .map_err(|err| EndpointError::network(err.to_string()))?
            };
            Ok(value)
        })
        .await
        .map_err(|_| EndpointError::timeout())??;

        Ok(value)
    }

    fn log_ssrf_block(
        &self,
        rule: &CompiledNetworkRule,
        url: &str,
        reason: &str,
        request_context: Option<&RequestContext>,
    ) {
        let log = build_ssrf_audit_log(rule, url, reason, request_context);
        tracing::warn!(
            target: "rulemorph_endpoint::ssrf",
            tenant_id = log.tenant_id,
            rule_ref = log.rule_ref,
            method = %log.method,
            url = log.url,
            reason = log.reason,
            "blocked ssrf request"
        );
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

    fn build_reply(
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

    fn build_context_json(&self, request_context: Option<&RequestContext>) -> JsonValue {
        let mut value = json!({
            "config": {
                "internal_base": self.config.internal_base,
            }
        });
        if let Some(request_context) = request_context {
            if let Some(tenant_id) = request_context.tenant_id.as_ref() {
                if let JsonValue::Object(ref mut map) = value {
                    map.insert(
                        "tenant_id".to_string(),
                        JsonValue::String(tenant_id.clone()),
                    );
                }
            }
        }
        value
    }

    fn step_context(
        &self,
        base_context: &JsonValue,
        params: Option<&JsonValue>,
        error: Option<&EndpointError>,
    ) -> JsonValue {
        let mut value = base_context.clone();
        if let Some(params) = params {
            if let JsonValue::Object(ref mut map) = value {
                map.insert("params".to_string(), params.clone());
            }
        }
        if let Some(error) = error {
            if let JsonValue::Object(ref mut map) = value {
                map.insert("error".to_string(), error.to_json());
            }
        }
        value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SsrAuditLog {
    tenant_id: String,
    rule_ref: String,
    method: Method,
    url: String,
    reason: String,
}

fn redact_ssrf_url(url: &str) -> String {
    let Ok(parsed) = url::Url::parse(url) else {
        return url.to_string();
    };
    let host = match parsed.host_str() {
        Some(host) => host,
        None => return url.to_string(),
    };
    let port = parsed
        .port()
        .map(|value| format!(":{value}"))
        .unwrap_or_default();
    format!("{}://{}{}{}", parsed.scheme(), host, port, parsed.path())
}

fn build_ssrf_audit_log(
    rule: &CompiledNetworkRule,
    url: &str,
    reason: &str,
    request_context: Option<&RequestContext>,
) -> SsrAuditLog {
    let tenant_id = request_context
        .and_then(|ctx| ctx.tenant_id.as_ref())
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    let rule_ref = rule
        .rule_ref
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    SsrAuditLog {
        tenant_id,
        rule_ref,
        method: rule.request.method.clone(),
        url: redact_ssrf_url(url),
        reason: reason.to_string(),
    }
}

#[derive(Debug)]
struct CompiledEndpointRule {
    base_dir: PathBuf,
    source_path: PathBuf,
    endpoints: Vec<CompiledEndpoint>,
}

impl CompiledEndpointRule {
    fn compile(raw: EndpointRuleFile, source_path: &Path) -> Result<Self> {
        let base_dir = source_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let endpoints = raw
            .endpoints
            .into_iter()
            .map(|endpoint| CompiledEndpoint::compile(endpoint, &base_dir))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            base_dir,
            source_path: source_path.to_path_buf(),
            endpoints,
        })
    }

    fn match_endpoint(&self, method: &Method, path: &str) -> Option<EndpointMatch<'_>> {
        self.endpoints
            .iter()
            .find(|endpoint| endpoint.matches(method, path))
            .map(|endpoint| EndpointMatch {
                params: endpoint.matcher.capture(path),
                endpoint,
            })
    }
}

struct EndpointMatch<'a> {
    endpoint: &'a CompiledEndpoint,
    params: HashMap<String, String>,
}

#[derive(Debug)]
struct CompiledEndpoint {
    method: Method,
    matcher: EndpointPath,
    input: Option<Vec<Mapping>>,
    steps: Vec<CompiledStep>,
    reply: CompiledReply,
    catch: Option<CatchSpec>,
}

impl CompiledEndpoint {
    fn compile(raw: EndpointDef, _base_dir: &Path) -> Result<Self> {
        let method =
            Method::from_bytes(raw.method.as_bytes()).map_err(|_| anyhow!("invalid method"))?;
        let matcher = EndpointPath::parse(&raw.path)?;
        let steps = raw
            .steps
            .into_iter()
            .map(CompiledStep::compile)
            .collect::<Result<Vec<_>>>()?;
        let reply = CompiledReply::compile(raw.reply)?;
        Ok(Self {
            method,
            matcher,
            input: raw.input,
            steps,
            reply,
            catch: raw.catch.map(CatchSpec::from),
        })
    }

    fn matches(&self, method: &Method, path: &str) -> bool {
        if &self.method != method {
            return false;
        }
        self.matcher.matches(path)
    }
}

#[derive(Debug)]
struct CompiledStep {
    rule: String,
    with: Option<JsonValue>,
    when: Option<rulemorph::v2_model::V2Condition>,
    catch: Option<CatchSpec>,
}

impl CompiledStep {
    fn compile(raw: EndpointStep) -> Result<Self> {
        let when = match raw.when {
            Some(value) => Some(parse_v2_condition(&value).map_err(|err| anyhow!(err))?),
            None => None,
        };
        Ok(Self {
            rule: raw.rule,
            with: raw.with,
            when,
            catch: raw.catch.map(CatchSpec::from),
        })
    }
}

#[derive(Debug)]
struct CompiledReply {
    status: rulemorph::v2_model::V2Expr,
    headers: HashMap<String, String>,
    body: Option<rulemorph::v2_model::V2Expr>,
}

impl CompiledReply {
    fn compile(raw: EndpointReply) -> Result<Self> {
        let status = parse_v2_expr(&raw.status).map_err(|err| anyhow!(err))?;
        let body = match raw.body {
            Some(value) => Some(parse_v2_expr(&value).map_err(|err| anyhow!(err))?),
            None => None,
        };
        let headers = raw
            .headers
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect();
        Ok(Self {
            status,
            headers,
            body,
        })
    }
}

#[derive(Debug)]
struct EndpointPath {
    segments: Vec<PathSegment>,
}

#[derive(Debug)]
enum PathSegment {
    Literal(String),
    Param(String),
}

impl EndpointPath {
    fn parse(path: &str) -> Result<Self> {
        if !path.starts_with('/') {
            return Err(anyhow!("endpoint path must start with /"));
        }
        let segments = path
            .trim_start_matches('/')
            .split('/')
            .filter(|seg| !seg.is_empty())
            .map(|seg| {
                if let Some(param) = seg.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
                    if param.is_empty() {
                        return Err(anyhow!("empty path param"));
                    }
                    Ok(PathSegment::Param(param.to_string()))
                } else {
                    Ok(PathSegment::Literal(seg.to_string()))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { segments })
    }

    fn matches(&self, path: &str) -> bool {
        let parts: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|seg| !seg.is_empty())
            .collect();
        if parts.len() != self.segments.len() {
            return false;
        }
        for (seg, part) in self.segments.iter().zip(parts.iter()) {
            match seg {
                PathSegment::Literal(lit) if lit != part => return false,
                _ => {}
            }
        }
        true
    }

    fn capture(&self, path: &str) -> HashMap<String, String> {
        let parts: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|seg| !seg.is_empty())
            .collect();
        let mut params = HashMap::new();
        for (seg, part) in self.segments.iter().zip(parts.iter()) {
            if let PathSegment::Param(name) = seg {
                params.insert(name.clone(), (*part).to_string());
            }
        }
        params
    }
}

#[derive(Debug, Clone, Deserialize)]
struct EndpointRuleFile {
    version: u8,
    #[serde(rename = "type")]
    rule_type: String,
    endpoints: Vec<EndpointDef>,
}

#[derive(Debug, Clone, Deserialize)]
struct EndpointDef {
    method: String,
    path: String,
    #[serde(default)]
    input: Option<Vec<Mapping>>,
    steps: Vec<EndpointStep>,
    reply: EndpointReply,
    #[serde(default)]
    catch: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct EndpointStep {
    rule: String,
    #[serde(default)]
    with: Option<JsonValue>,
    #[serde(default)]
    when: Option<JsonValue>,
    #[serde(default)]
    catch: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct EndpointReply {
    status: JsonValue,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
    #[serde(default)]
    body: Option<JsonValue>,
}

#[derive(Debug)]
struct CompiledNetworkRule {
    request: CompiledNetworkRequest,
    timeout: Duration,
    select: Option<String>,
    body: Option<rulemorph::v2_model::V2Expr>,
    body_map: Option<Vec<Mapping>>,
    body_rule: Option<LoadedRule>,
    body_rule_ref: Option<String>,
    rule_ref: Option<String>,
    catch: Option<CatchSpec>,
    retry: Option<RetryConfig>,
    internal_auth: bool,
    base_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
struct NetworkRuleFile {
    version: u8,
    #[serde(rename = "type")]
    rule_type: String,
    request: NetworkRequest,
    timeout: String,
    #[serde(default)]
    internal_auth: bool,
    #[serde(default)]
    select: Option<String>,
    #[serde(default)]
    body: Option<JsonValue>,
    #[serde(default)]
    body_map: Option<Vec<Mapping>>,
    #[serde(default)]
    body_rule: Option<String>,
    #[serde(default)]
    catch: Option<HashMap<String, String>>,
    #[serde(default)]
    retry: Option<NetworkRetry>,
}

#[derive(Debug, Deserialize)]
struct NetworkRequest {
    method: String,
    url: JsonValue,
    #[serde(default)]
    headers: Option<HashMap<String, JsonValue>>,
}

#[derive(Debug, Deserialize)]
struct NetworkRetry {
    #[serde(default)]
    max: Option<u32>,
    #[serde(default)]
    backoff: Option<String>,
    #[serde(default)]
    initial_delay: Option<String>,
}

#[derive(Debug, Clone)]
struct RetryConfig {
    max: u32,
    backoff: RetryBackoff,
    initial_delay: Duration,
}

#[derive(Debug, Clone, Copy)]
enum RetryBackoff {
    Fixed,
    Linear,
    Exponential,
}

#[derive(Debug)]
struct CompiledNetworkRequest {
    method: Method,
    url: rulemorph::v2_model::V2Expr,
    headers: HashMap<String, rulemorph::v2_model::V2Expr>,
}

#[derive(Debug)]
struct CatchSpec(HashMap<String, String>);

impl From<HashMap<String, String>> for CatchSpec {
    fn from(value: HashMap<String, String>) -> Self {
        CatchSpec(value)
    }
}

impl CatchSpec {
    fn match_target(&self, error: &EndpointError) -> Option<PathBuf> {
        let map = &self.0;
        if let Some(status) = error.status {
            let key = status.to_string();
            if let Some(value) = map.get(&key) {
                return Some(PathBuf::from(value));
            }
            let pattern = if (400..500).contains(&status) {
                "4xx"
            } else if (500..600).contains(&status) {
                "5xx"
            } else {
                ""
            };
            if !pattern.is_empty() {
                if let Some(value) = map.get(pattern) {
                    return Some(PathBuf::from(value));
                }
            }
        }
        if error.kind == EndpointErrorKind::Timeout {
            if let Some(value) = map.get("timeout") {
                return Some(PathBuf::from(value));
            }
        }
        map.get("default").map(PathBuf::from)
    }
}

fn build_input(
    parts: &axum::http::request::Parts,
    path_params: &HashMap<String, String>,
    body: Option<JsonValue>,
) -> Result<JsonValue, EndpointError> {
    let query = parse_query(parts.uri.query())?;
    Ok(build_input_from_parts(parts, path_params, body, query))
}

fn build_input_from_parts(
    parts: &axum::http::request::Parts,
    path_params: &HashMap<String, String>,
    body: Option<JsonValue>,
    query: JsonValue,
) -> JsonValue {
    let mut headers: HashMap<String, String> = HashMap::new();
    for (name, value) in parts.headers.iter() {
        let key = name.as_str().to_lowercase();
        let value = value.to_str().unwrap_or_default();
        if let Some(existing) = headers.get_mut(&key) {
            existing.push(',');
            existing.push_str(value);
        } else {
            headers.insert(key, value.to_string());
        }
    }

    let mut input = json!({
        "method": parts.method.as_str(),
        "path": path_params,
        "query": query,
        "headers": headers,
    });

    if let Some(body) = body {
        if let JsonValue::Object(ref mut map) = input {
            map.insert("body".to_string(), body);
        }
    }

    input
}

fn is_multipart_form_data(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| multer::parse_boundary(value).is_ok())
}

fn is_multipart_import_request(method: &Method, path: &str, headers: &HeaderMap) -> bool {
    method == Method::POST && path == "/api/import" && is_multipart_form_data(headers)
}

fn internal_hosts_match(target: Option<&str>, base: Option<&str>) -> bool {
    let Some(target) = target.map(normalize_internal_host) else {
        return false;
    };
    let Some(base) = base.map(normalize_internal_host) else {
        return false;
    };
    target == base || is_loopback_host(&target) && is_loopback_host(&base)
}

fn normalize_internal_host(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn build_headers(
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

fn parse_query(query: Option<&str>) -> Result<JsonValue, EndpointError> {
    let mut map: HashMap<String, String> = HashMap::new();
    if let Some(q) = query {
        for (key, value) in url::form_urlencoded::parse(q.as_bytes()) {
            let key = key.into_owned();
            let value = value.into_owned();
            if map.contains_key(&key) {
                return Err(EndpointError::invalid(format!(
                    "duplicate query param: {}",
                    key
                )));
            }
            map.insert(key, value);
        }
    }
    serde_json::to_value(map).map_err(|err| EndpointError::invalid(err.to_string()))
}

fn apply_mappings_via_rule(
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
        },
        output: None,
        record_when: None,
        mappings: mappings.to_vec(),
        steps: None,
        finalize: None,
    };
    transform_record(&rule, record, context)
}

fn eval_expr_value(
    expr: &rulemorph::v2_model::V2Expr,
    input: &JsonValue,
    context: Option<&JsonValue>,
) -> Result<EvalValue> {
    let ctx = V2EvalContext::new();
    eval_v2_expr(expr, input, context, &empty_object(), "expr", &ctx)
        .map_err(|err| anyhow!(err.to_string()))
}

fn eval_expr_string(
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

fn resolve_rule_path(base_dir: &Path, rule: &str) -> PathBuf {
    let path = PathBuf::from(rule);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
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

fn compile_network_rule(raw: NetworkRuleFile, path: &Path) -> Result<CompiledNetworkRule> {
    if raw.version != 2 {
        return Err(anyhow!("network rule version must be 2"));
    }
    if raw.rule_type != "network" {
        return Err(anyhow!("network rule type must be network"));
    }
    if raw.body.is_some() && raw.body_map.is_some() {
        return Err(anyhow!("body and body_map are mutually exclusive"));
    }
    if raw.body.is_some() && raw.body_rule.is_some() {
        return Err(anyhow!("body and body_rule are mutually exclusive"));
    }
    if raw.body_map.is_some() && raw.body_rule.is_some() {
        return Err(anyhow!("body_map and body_rule are mutually exclusive"));
    }

    let method =
        Method::from_bytes(raw.request.method.as_bytes()).map_err(|_| anyhow!("invalid method"))?;
    if method == Method::GET
        && (raw.body.is_some() || raw.body_map.is_some() || raw.body_rule.is_some())
    {
        return Err(anyhow!("GET with body is not allowed"));
    }
    let url_expr = parse_v2_expr(&raw.request.url).map_err(|err| anyhow!(err))?;
    let mut headers: HashMap<String, rulemorph::v2_model::V2Expr> = HashMap::new();
    for (key, value) in raw.request.headers.unwrap_or_default() {
        let expr = parse_v2_expr(&value).map_err(|err| anyhow!(err))?;
        headers.insert(key.to_lowercase(), expr);
    }
    let timeout = parse_duration(&raw.timeout)?;
    if timeout.is_zero() {
        return Err(anyhow!("timeout must be > 0"));
    }
    let body = match raw.body {
        Some(value) => Some(parse_v2_expr(&value).map_err(|err| anyhow!(err))?),
        None => None,
    };
    let body_rule = match raw.body_rule.as_ref() {
        Some(path_str) => {
            let resolved =
                resolve_rule_path(path.parent().unwrap_or_else(|| Path::new(".")), path_str);
            let source = std::fs::read_to_string(&resolved)
                .with_context(|| format!("failed to read {}", resolved.display()))?;
            let rule = parse_rule_file_with_format(&source, RuleFormat::from_path(&resolved))
                .with_context(|| format!("failed to parse {}", resolved.display()))?;
            validate_rule_file_with_source(&rule, &source)
                .map_err(|err| anyhow!("failed to validate {}: {:?}", resolved.display(), err))?;
            let base_dir = resolved
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf();
            Some(LoadedRule { rule, base_dir })
        }
        None => None,
    };
    let body_rule_ref = raw.body_rule.as_ref().map(|path_str| {
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let resolved = resolve_rule_path(base_dir, path_str);
        rule_ref_from_path(base_dir, &resolved)
    });

    let retry = compile_retry(raw.retry.as_ref())?;
    let rule_ref = {
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        Some(rule_ref_from_path(base_dir, path))
    };
    Ok(CompiledNetworkRule {
        request: CompiledNetworkRequest {
            method,
            url: url_expr,
            headers,
        },
        timeout,
        select: raw.select,
        body,
        body_map: raw.body_map,
        body_rule,
        body_rule_ref,
        rule_ref,
        catch: raw.catch.map(CatchSpec::from),
        retry,
        internal_auth: raw.internal_auth,
        base_dir: path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf(),
    })
}

fn parse_duration(value: &str) -> Result<Duration> {
    let trimmed = value.trim();
    if let Some(ms) = trimmed.strip_suffix("ms") {
        let amount = u64::from_str(ms.trim()).context("invalid ms")?;
        return Ok(Duration::from_millis(amount));
    }
    if let Some(sec) = trimmed.strip_suffix('s') {
        let amount = u64::from_str(sec.trim()).context("invalid s")?;
        return Ok(Duration::from_secs(amount));
    }
    Err(anyhow!("invalid duration: {}", value))
}

fn compile_retry(raw: Option<&NetworkRetry>) -> Result<Option<RetryConfig>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let max = raw.max.unwrap_or(0);
    if max == 0 {
        return Ok(None);
    }
    let backoff = match raw.backoff.as_deref().unwrap_or("fixed") {
        "fixed" => RetryBackoff::Fixed,
        "linear" => RetryBackoff::Linear,
        "exponential" => RetryBackoff::Exponential,
        other => return Err(anyhow!("invalid retry backoff: {}", other)),
    };
    let initial_delay = match raw.initial_delay.as_deref() {
        Some(value) => parse_duration(value)?,
        None => Duration::from_millis(100),
    };
    Ok(Some(RetryConfig {
        max,
        backoff,
        initial_delay,
    }))
}

impl RetryConfig {
    fn delay_for(&self, attempt: u32) -> Duration {
        let factor = attempt.saturating_add(1);
        match self.backoff {
            RetryBackoff::Fixed => self.initial_delay,
            RetryBackoff::Linear => self
                .initial_delay
                .checked_mul(factor)
                .unwrap_or(Duration::MAX),
            RetryBackoff::Exponential => {
                let exp = 2u32.saturating_pow(attempt);
                self.initial_delay.checked_mul(exp).unwrap_or(Duration::MAX)
            }
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

fn rule_ref_from_rule(base_dir: &Path, rule: &str) -> String {
    let resolved = resolve_rule_path(base_dir, rule);
    rule_ref_from_path(base_dir, &resolved)
}

fn safe_rule_ref_from_path(base_dir: &Path, path: &Path) -> Option<String> {
    if path.strip_prefix(base_dir).is_ok() {
        Some(rule_ref_from_path(base_dir, path))
    } else {
        None
    }
}

fn rule_ref_from_path(base_dir: &Path, path: &Path) -> String {
    if let Ok(rel) = path.strip_prefix(base_dir) {
        let rel = rel.to_string_lossy().replace('\\', "/");
        if rel.starts_with("rules/") {
            rel
        } else {
            format!("rules/{}", rel)
        }
    } else {
        path.display().to_string()
    }
}

fn rule_display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("rule")
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
