use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use chrono::Utc;
use futures_util::stream;
use http_body_util::LengthLimitError;
use reqwest::Client;
use rulemorph::PathToken;
use rulemorph::v2_eval::{
    EvalValue, V2EvalContext, eval_v2_condition, eval_v2_expr, eval_v2_if_step, eval_v2_let_step,
    eval_v2_map_step, eval_v2_op_step, eval_v2_pipe, eval_v2_ref, eval_v2_start,
};
use rulemorph::v2_model::{V2Ref, V2Start, V2Step};
use rulemorph::v2_parser::{
    is_literal_escape, is_pipe_value, is_v2_ref, parse_v2_condition, parse_v2_expr,
    parse_v2_pipe_from_value,
};
use rulemorph::{
    Expr, Mapping, RuleError, RuleFile, TransformError, TransformErrorKind, get_path, parse_path,
    parse_rule_file, transform_record, transform_record_with_base_dir,
    validate_rule_file_with_source,
};
use rulemorph_trace::{TraceWriteOptions, TraceWriter, TraceWriterConfig};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tracing::warn;
use zip::ZipArchive;

const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;
const MULTIPART_IMPORT_MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
const MULTIPART_IMPORT_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const MULTIPART_IMPORT_MAX_ENTRIES: usize = 4096;
use uuid::Uuid;

use crate::ssrf::{ResolvedSsrfTarget, resolve_ssrf_target};

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

#[derive(Debug, Clone)]
pub struct RulesDirError {
    pub code: String,
    pub file: PathBuf,
    pub path: Option<String>,
    pub line: Option<usize>,
    pub column: Option<usize>,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct RulesDirErrors {
    pub errors: Vec<RulesDirError>,
}

impl fmt::Display for RulesDirErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, err) in self.errors.iter().enumerate() {
            if index > 0 {
                writeln!(f)?;
            }
            let mut parts = Vec::new();
            parts.push(format!("E {}", err.code));
            parts.push(format!("file={}", err.file.display()));
            if let Some(path) = &err.path {
                parts.push(format!("path={}", path));
            }
            if let Some(line) = err.line {
                parts.push(format!("line={}", line));
            }
            if let Some(column) = err.column {
                parts.push(format!("col={}", column));
            }
            parts.push(format!("msg=\"{}\"", err.message));
            write!(f, "{}", parts.join(" "))?;
        }
        Ok(())
    }
}

impl std::error::Error for RulesDirErrors {}

#[derive(Debug, Default, Clone, Copy)]
struct RuleRefUsage {
    step: bool,
    body_rule: bool,
    catch_rule: bool,
    branch_rule: bool,
}

impl RuleRefUsage {
    fn step() -> Self {
        RuleRefUsage {
            step: true,
            ..RuleRefUsage::default()
        }
    }

    fn body_rule() -> Self {
        RuleRefUsage {
            body_rule: true,
            ..RuleRefUsage::default()
        }
    }

    fn catch_rule() -> Self {
        RuleRefUsage {
            catch_rule: true,
            ..RuleRefUsage::default()
        }
    }

    fn branch_rule() -> Self {
        RuleRefUsage {
            branch_rule: true,
            ..RuleRefUsage::default()
        }
    }

    fn merge(&mut self, other: RuleRefUsage) {
        self.step |= other.step;
        self.body_rule |= other.body_rule;
        self.catch_rule |= other.catch_rule;
        self.branch_rule |= other.branch_rule;
    }
}

#[derive(Debug, Default)]
struct ValidationState {
    validated_content: BTreeSet<PathBuf>,
}

pub fn validate_rules_dir(rules_dir: &Path) -> std::result::Result<(), RulesDirErrors> {
    let mut errors = Vec::new();
    let endpoint_path = rules_dir.join("endpoint.yaml");
    let source = match read_rule_source(&endpoint_path, &mut errors) {
        Some(source) => source,
        None => return Err(RulesDirErrors { errors }),
    };

    let raw: EndpointRuleFile = match parse_yaml(&endpoint_path, &source, &mut errors) {
        Some(raw) => raw,
        None => return Err(RulesDirErrors { errors }),
    };

    if raw.version != 2 {
        push_error(
            &mut errors,
            "InvalidVersion",
            &endpoint_path,
            "endpoint rule version must be 2",
            Some("version".to_string()),
            None,
        );
    }
    if raw.rule_type != "endpoint" {
        push_error(
            &mut errors,
            "InvalidRuleType",
            &endpoint_path,
            "endpoint rule type must be endpoint",
            Some("type".to_string()),
            None,
        );
    }
    if let Err(err) = CompiledEndpointRule::compile(raw.clone(), &endpoint_path) {
        push_error(
            &mut errors,
            "EndpointCompileFailed",
            &endpoint_path,
            err.to_string(),
            None,
            None,
        );
    }

    let base_dir = endpoint_path.parent().unwrap_or_else(|| Path::new("."));
    let mut refs: BTreeSet<PathBuf> = BTreeSet::new();
    let mut ref_usage: HashMap<PathBuf, RuleRefUsage> = HashMap::new();
    for endpoint in &raw.endpoints {
        for step in &endpoint.steps {
            let resolved = resolve_rule_path(base_dir, &step.rule);
            refs.insert(resolved.clone());
            ref_usage
                .entry(resolved)
                .and_modify(|usage| usage.merge(RuleRefUsage::step()))
                .or_insert_with(RuleRefUsage::step);
            if let Some(catch) = &step.catch {
                for target in catch.values() {
                    let resolved = resolve_rule_path(base_dir, target);
                    refs.insert(resolved.clone());
                    ref_usage
                        .entry(resolved)
                        .and_modify(|usage| usage.merge(RuleRefUsage::catch_rule()))
                        .or_insert_with(RuleRefUsage::catch_rule);
                }
            }
        }
        if let Some(catch) = &endpoint.catch {
            for target in catch.values() {
                let resolved = resolve_rule_path(base_dir, target);
                refs.insert(resolved.clone());
                ref_usage
                    .entry(resolved)
                    .and_modify(|usage| usage.merge(RuleRefUsage::catch_rule()))
                    .or_insert_with(RuleRefUsage::catch_rule);
            }
        }
    }

    let mut state = ValidationState::default();
    for path in refs {
        let usage = ref_usage.get(&path).copied().unwrap_or_default();
        validate_rule_path(&path, usage, &mut state, &mut errors);
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(RulesDirErrors { errors })
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
        let raw_source: serde_yaml::Value = serde_yaml::from_str(&source)
            .with_context(|| format!("failed to parse {}", endpoint_path.display()))?;
        let raw_rule_source = serde_json::to_value(raw_source).unwrap_or_else(|_| json!({}));
        let raw: EndpointRuleFile = serde_yaml::from_str(&source)
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
        let body_limit = if is_multipart {
            MULTIPART_IMPORT_MAX_TOTAL_BYTES as usize
        } else {
            self.config.max_body_bytes
        };
        let body_bytes = match axum::body::to_bytes(body, body_limit).await {
            Ok(bytes) => Ok(bytes),
            Err(err) => {
                if is_length_limit_error(&err) {
                    Err(EndpointError::payload_too_large(body_limit))
                } else {
                    Err(EndpointError::network(format!(
                        "request body read error: {}",
                        err
                    )))
                }
            }
        };
        let mut _multipart_temp_dir: Option<tempfile::TempDir> = None;
        let body_value = match body_bytes {
            Ok(body_bytes) => {
                if is_multipart {
                    match build_multipart_import_body(&parts.headers, body_bytes).await {
                        Ok((body, temp_dir)) => {
                            _multipart_temp_dir = Some(temp_dir);
                            Ok(Some(body))
                        }
                        Err(err) => Err(err),
                    }
                } else {
                    if body_bytes.is_empty() {
                        Ok(None)
                    } else {
                        serde_json::from_slice::<JsonValue>(&body_bytes)
                            .map(Some)
                            .map_err(|err| EndpointError::invalid(err.to_string()))
                    }
                }
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

#[derive(Debug, Clone)]
struct EndpointError {
    kind: EndpointErrorKind,
    status: Option<u16>,
    message: String,
    path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EndpointErrorKind {
    Timeout,
    HttpStatus,
    Network,
    Transform,
    Invalid,
}

impl EndpointError {
    fn timeout() -> Self {
        Self {
            kind: EndpointErrorKind::Timeout,
            status: None,
            message: "timeout".to_string(),
            path: None,
        }
    }

    fn http_status(status: u16) -> Self {
        Self {
            kind: EndpointErrorKind::HttpStatus,
            status: Some(status),
            message: format!("http status {}", status),
            path: None,
        }
    }

    fn network(message: String) -> Self {
        Self {
            kind: EndpointErrorKind::Network,
            status: None,
            message,
            path: None,
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: EndpointErrorKind::Invalid,
            status: None,
            message: message.into(),
            path: None,
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            kind: EndpointErrorKind::Invalid,
            status: Some(StatusCode::BAD_REQUEST.as_u16()),
            message: message.into(),
            path: None,
        }
    }

    fn payload_too_large(limit: usize) -> Self {
        Self {
            kind: EndpointErrorKind::Invalid,
            status: Some(StatusCode::PAYLOAD_TOO_LARGE.as_u16()),
            message: format!("payload too large (limit {} bytes)", limit),
            path: None,
        }
    }

    fn from_transform(err: TransformError) -> Self {
        Self {
            kind: EndpointErrorKind::Transform,
            status: None,
            message: err.to_string(),
            path: None,
        }
    }

    fn with_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    fn to_json(&self) -> JsonValue {
        json!({
            "kind": format!("{:?}", self.kind),
            "status": self.status,
            "message": self.message,
            "path": self.path.as_ref().map(|p| p.display().to_string()),
        })
    }
}

impl std::fmt::Display for EndpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
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

async fn build_multipart_import_body(
    headers: &HeaderMap,
    body_bytes: Bytes,
) -> Result<(JsonValue, tempfile::TempDir), EndpointError> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| EndpointError::bad_request("missing content-type"))?;
    let boundary = multer::parse_boundary(content_type)
        .map_err(|err| EndpointError::bad_request(format!("multipart error: {}", err)))?;
    let stream = stream::once(async move { Ok::<Bytes, std::io::Error>(body_bytes) });
    let mut multipart = multer::Multipart::new(stream, boundary);
    let mut zip_file: Option<tempfile::NamedTempFile> = None;
    let mut total_bytes: u64 = 0;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|err| EndpointError::bad_request(format!("multipart error: {}", err)))?
    {
        if field.name() != Some("bundle") {
            continue;
        }
        let mut handle = tempfile::NamedTempFile::new()
            .map_err(|err| EndpointError::network(err.to_string()))?;
        let mut field = field;
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|err| EndpointError::bad_request(format!("upload error: {}", err)))?
        {
            total_bytes = total_bytes.saturating_add(chunk.len() as u64);
            if total_bytes > MULTIPART_IMPORT_MAX_TOTAL_BYTES {
                return Err(EndpointError::payload_too_large(
                    MULTIPART_IMPORT_MAX_TOTAL_BYTES as usize,
                ));
            }
            handle
                .write_all(&chunk)
                .map_err(|err| EndpointError::network(err.to_string()))?;
        }
        zip_file = Some(handle);
        break;
    }

    let zip_file = zip_file.ok_or_else(|| EndpointError::bad_request("missing bundle file"))?;
    let extract_dir =
        tempfile::TempDir::new().map_err(|err| EndpointError::network(err.to_string()))?;
    extract_zip(zip_file.path(), extract_dir.path()).map_err(EndpointError::bad_request)?;
    let bundle_root = resolve_bundle_root(extract_dir.path())?;
    Ok((
        json!({ "bundle_path": bundle_root.display().to_string() }),
        extract_dir,
    ))
}

fn extract_zip(path: &Path, dest: &Path) -> Result<(), String> {
    let file = File::open(path).map_err(|err| format!("failed to open zip: {}", err))?;
    let mut archive = ZipArchive::new(file).map_err(|err| format!("invalid zip: {}", err))?;
    let mut total_bytes: u64 = 0;

    if archive.len() > MULTIPART_IMPORT_MAX_ENTRIES {
        return Err("zip has too many entries".to_string());
    }

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|err| format!("zip entry error: {}", err))?;
        let name = entry.name().to_string();
        let entry_path = Path::new(&name);
        for component in entry_path.components() {
            match component {
                std::path::Component::Normal(_) => {}
                _ => return Err(format!("invalid zip entry path: {}", name)),
            }
        }
        if let Some(mode) = entry.unix_mode() {
            if (mode & 0o170000) == 0o120000 {
                return Err(format!("zip entry is symlink: {}", name));
            }
        }
        let out_path = dest.join(entry_path);
        if entry.is_dir() {
            std::fs::create_dir_all(&out_path)
                .map_err(|err| format!("failed to create dir: {}", err))?;
            continue;
        }
        let size = entry.size();
        if size > MULTIPART_IMPORT_MAX_FILE_BYTES {
            return Err(format!("zip entry too large: {}", name));
        }
        total_bytes = total_bytes.saturating_add(size);
        if total_bytes > MULTIPART_IMPORT_MAX_TOTAL_BYTES {
            return Err("zip exceeds max total bytes".to_string());
        }
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create dir: {}", err))?;
        }
        let mut outfile = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&out_path)
            .map_err(|err| format!("failed to create file: {}", err))?;
        let copied =
            copy_zip_entry_bounded(&mut entry, &mut outfile, MULTIPART_IMPORT_MAX_FILE_BYTES)
                .map_err(|err| format!("failed to write file: {}", err))?;
        total_bytes = total_bytes.saturating_sub(size).saturating_add(copied);
        if copied > MULTIPART_IMPORT_MAX_FILE_BYTES {
            return Err(format!("zip entry too large: {}", name));
        }
        if total_bytes > MULTIPART_IMPORT_MAX_TOTAL_BYTES {
            return Err("zip exceeds max total bytes".to_string());
        }
    }
    Ok(())
}

fn copy_zip_entry_bounded<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    max_bytes: u64,
) -> std::io::Result<u64> {
    let mut copied = 0u64;
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(copied);
        }
        copied = copied.saturating_add(read as u64);
        if copied > max_bytes {
            return Ok(copied);
        }
        writer.write_all(&buffer[..read])?;
    }
}

fn resolve_bundle_root(base: &Path) -> Result<PathBuf, EndpointError> {
    if base.join("traces").exists() || base.join("rules").exists() {
        return Ok(base.to_path_buf());
    }
    let mut entries = std::fs::read_dir(base)
        .map_err(|err| EndpointError::bad_request(format!("invalid zip bundle: {}", err)))?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>();
    if entries.len() == 1 {
        let entry = entries.remove(0);
        let path = entry.path();
        if path.is_dir() && (path.join("traces").exists() || path.join("rules").exists()) {
            return Ok(path);
        }
    }
    Err(EndpointError::bad_request(
        "zip bundle must include traces/ or rules/",
    ))
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

fn read_rule_source(path: &Path, errors: &mut Vec<RulesDirError>) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(source) => Some(source),
        Err(err) => {
            push_error(errors, "ReadFailed", path, err.to_string(), None, None);
            None
        }
    }
}

fn parse_yaml<T: DeserializeOwned>(
    path: &Path,
    source: &str,
    errors: &mut Vec<RulesDirError>,
) -> Option<T> {
    match serde_yaml::from_str(source) {
        Ok(value) => Some(value),
        Err(err) => {
            push_yaml_error(errors, path, &err);
            None
        }
    }
}

fn parse_rule_type(path: &Path, source: &str, errors: &mut Vec<RulesDirError>) -> Option<String> {
    let meta: serde_yaml::Value = match serde_yaml::from_str(source) {
        Ok(value) => value,
        Err(err) => {
            push_yaml_error(errors, path, &err);
            return None;
        }
    };
    Some(
        meta.get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("normal")
            .to_string(),
    )
}

fn push_yaml_error(errors: &mut Vec<RulesDirError>, path: &Path, err: &serde_yaml::Error) {
    let location = err.location().map(|loc| (loc.line(), loc.column()));
    push_error(
        errors,
        "YamlParseFailed",
        path,
        err.to_string(),
        None,
        location,
    );
}

fn push_error(
    errors: &mut Vec<RulesDirError>,
    code: impl Into<String>,
    file: &Path,
    message: impl Into<String>,
    path: Option<String>,
    location: Option<(usize, usize)>,
) {
    let (line, column) = location
        .map(|(line, column)| (Some(line), Some(column)))
        .unwrap_or((None, None));
    errors.push(RulesDirError {
        code: code.into(),
        file: file.to_path_buf(),
        path,
        line,
        column,
        message: message.into(),
    });
}

fn push_rule_error(errors: &mut Vec<RulesDirError>, path: &Path, err: &RuleError) {
    let location = err.location.as_ref().map(|loc| (loc.line, loc.column));
    push_error(
        errors,
        err.code.as_str(),
        path,
        err.message.clone(),
        err.path.clone(),
        location,
    );
}

fn validate_rule_path(
    path: &Path,
    usage: RuleRefUsage,
    state: &mut ValidationState,
    errors: &mut Vec<RulesDirError>,
) {
    let source = match read_rule_source(path, errors) {
        Some(source) => source,
        None => return,
    };
    let rule_type = match parse_rule_type(path, &source, errors) {
        Some(rule_type) => rule_type,
        None => return,
    };

    if usage.step && rule_type == "endpoint" {
        push_error(
            errors,
            "EndpointRuleNotAllowed",
            path,
            "endpoint rule not allowed as step",
            Some("type".to_string()),
            None,
        );
    }
    if usage.body_rule && rule_type != "normal" {
        push_error(
            errors,
            "BodyRuleInvalid",
            path,
            "body_rule must be normal",
            Some("type".to_string()),
            None,
        );
    }
    if usage.catch_rule && rule_type != "normal" {
        push_error(
            errors,
            "CatchRuleInvalid",
            path,
            "catch rule must be normal",
            Some("type".to_string()),
            None,
        );
    }
    if usage.branch_rule && rule_type != "normal" {
        push_error(
            errors,
            "BranchRuleInvalid",
            path,
            "branch rule must be normal",
            Some("type".to_string()),
            None,
        );
    }

    if !state.validated_content.insert(path.to_path_buf()) {
        return;
    }

    match rule_type.as_str() {
        "network" => validate_network_rule(&source, path, state, errors),
        "endpoint" => {}
        _ => validate_normal_rule(&source, path, state, errors),
    }
}

fn validate_normal_rule(
    source: &str,
    path: &Path,
    state: &mut ValidationState,
    errors: &mut Vec<RulesDirError>,
) {
    let rule = match parse_rule_file(source) {
        Ok(rule) => rule,
        Err(err) => {
            push_yaml_error(errors, path, &err);
            return;
        }
    };
    if let Err(rule_errors) = validate_rule_file_with_source(&rule, source) {
        for err in rule_errors {
            push_rule_error(errors, path, &err);
        }
    }
    if let Some(steps) = &rule.steps {
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        for step in steps {
            if let Some(branch) = &step.branch {
                if !branch.then.trim().is_empty() {
                    let resolved = resolve_rule_path(base_dir, branch.then.as_str());
                    validate_rule_path(&resolved, RuleRefUsage::branch_rule(), state, errors);
                }
                if let Some(r#else) = &branch.r#else {
                    if !r#else.trim().is_empty() {
                        let resolved = resolve_rule_path(base_dir, r#else.as_str());
                        validate_rule_path(&resolved, RuleRefUsage::branch_rule(), state, errors);
                    }
                }
            }
        }
    }
}

fn validate_network_rule(
    source: &str,
    path: &Path,
    state: &mut ValidationState,
    errors: &mut Vec<RulesDirError>,
) {
    let raw: NetworkRuleFile = match parse_yaml(path, source, errors) {
        Some(raw) => raw,
        None => return,
    };

    if raw.version != 2 {
        push_error(
            errors,
            "InvalidVersion",
            path,
            "network rule version must be 2",
            Some("version".to_string()),
            None,
        );
    }
    if raw.rule_type != "network" {
        push_error(
            errors,
            "InvalidRuleType",
            path,
            "network rule type must be network",
            Some("type".to_string()),
            None,
        );
    }
    if raw.body.is_some() && raw.body_map.is_some() {
        push_error(
            errors,
            "NetworkInvalidConfig",
            path,
            "body and body_map are mutually exclusive",
            Some("body".to_string()),
            None,
        );
    }
    if raw.body.is_some() && raw.body_rule.is_some() {
        push_error(
            errors,
            "NetworkInvalidConfig",
            path,
            "body and body_rule are mutually exclusive",
            Some("body".to_string()),
            None,
        );
    }
    if raw.body_map.is_some() && raw.body_rule.is_some() {
        push_error(
            errors,
            "NetworkInvalidConfig",
            path,
            "body_map and body_rule are mutually exclusive",
            Some("body_map".to_string()),
            None,
        );
    }

    let method = match Method::from_bytes(raw.request.method.as_bytes()) {
        Ok(method) => Some(method),
        Err(_) => {
            push_error(
                errors,
                "InvalidMethod",
                path,
                "invalid method",
                Some("request.method".to_string()),
                None,
            );
            None
        }
    };

    if let Some(method) = method {
        if method == Method::GET
            && (raw.body.is_some() || raw.body_map.is_some() || raw.body_rule.is_some())
        {
            push_error(
                errors,
                "NetworkInvalidConfig",
                path,
                "GET with body is not allowed",
                Some("request.method".to_string()),
                None,
            );
        }
    }

    if let Err(err) = parse_v2_expr(&raw.request.url) {
        push_error(
            errors,
            "InvalidExpr",
            path,
            format!("request.url: {}", err),
            Some("request.url".to_string()),
            None,
        );
    }
    if let Some(body) = &raw.body {
        if let Err(err) = parse_v2_expr(body) {
            push_error(
                errors,
                "InvalidExpr",
                path,
                format!("body: {}", err),
                Some("body".to_string()),
                None,
            );
        }
    }
    if let Some(headers) = &raw.request.headers {
        for (key, value) in headers {
            if let Err(err) = parse_v2_expr(value) {
                let field = format!("request.headers.{}", key);
                push_error(
                    errors,
                    "InvalidExpr",
                    path,
                    format!("{}: {}", field, err),
                    Some(field),
                    None,
                );
            }
        }
    }

    match parse_duration(&raw.timeout) {
        Ok(timeout) => {
            if timeout.is_zero() {
                push_error(
                    errors,
                    "InvalidTimeout",
                    path,
                    "timeout must be > 0",
                    Some("timeout".to_string()),
                    None,
                );
            }
        }
        Err(err) => {
            push_error(
                errors,
                "InvalidTimeout",
                path,
                err.to_string(),
                Some("timeout".to_string()),
                None,
            );
        }
    }

    if let Err(err) = compile_retry(raw.retry.as_ref()) {
        push_error(
            errors,
            "InvalidRetry",
            path,
            err.to_string(),
            Some("retry".to_string()),
            None,
        );
    }

    let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
    if let Some(body_rule) = raw.body_rule.as_deref() {
        let resolved = resolve_rule_path(base_dir, body_rule);
        validate_rule_path(&resolved, RuleRefUsage::body_rule(), state, errors);
    }
    if let Some(catch) = &raw.catch {
        for target in catch.values() {
            let resolved = resolve_rule_path(base_dir, target);
            validate_rule_path(&resolved, RuleRefUsage::catch_rule(), state, errors);
        }
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
    let meta: serde_yaml::Value = serde_yaml::from_str(&source)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    let rule_type = meta
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("normal");
    match rule_type {
        "network" => {
            let raw: NetworkRuleFile = serde_yaml::from_str(&source)
                .with_context(|| format!("failed to parse {}", path.display()))?;
            let compiled = compile_network_rule(raw, path)?;
            Ok(RuleKind::Network(compiled))
        }
        "endpoint" => Err(anyhow!("endpoint rule not allowed as step")),
        _ => {
            let rule = parse_rule_file(&source)
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
            let rule = parse_rule_file(&source)
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
    let raw: serde_yaml::Value = serde_yaml::from_str(source).ok()?;
    serde_json::to_value(raw).ok()
}

fn build_rule_trace(
    rule_type: &str,
    name: String,
    path: String,
    version: u8,
    rule_source: JsonValue,
    input: JsonValue,
    output: JsonValue,
    nodes: Vec<JsonValue>,
    finalize: Option<JsonValue>,
    duration_us: u64,
    status: &str,
) -> JsonValue {
    let trace_id = Uuid::new_v4().to_string();
    let now = Utc::now();
    let record = json!({
        "index": 0,
        "status": status,
        "duration_us": duration_us,
        "input": input,
        "output": output,
        "nodes": nodes,
    });
    let mut trace = json!({
        "trace_id": trace_id,
        "timestamp": now.to_rfc3339(),
        "rule": {
            "type": rule_type,
            "name": name,
            "path": path,
            "version": version
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
    });
    if let Some(finalize) = finalize {
        if let Some(obj) = trace.as_object_mut() {
            obj.insert("finalize".to_string(), finalize);
        }
    }
    trace
}

struct RuleTraceNodes {
    nodes: Vec<JsonValue>,
    finalize: Option<JsonValue>,
    pre_finalize_output: Option<JsonValue>,
    duration_us: u64,
}

fn build_rule_nodes_from_rule(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> RuleTraceNodes {
    let mut nodes = Vec::new();
    let mut finalize_trace: Option<JsonValue> = None;
    let mut pre_finalize_output: Option<JsonValue> = None;
    if let Some(steps) = &rule.steps {
        let mut step_outputs = Vec::with_capacity(steps.len());
        for index in 0..steps.len() {
            let mut partial_rule = rule.clone();
            partial_rule.steps = Some(steps[..=index].to_vec());
            partial_rule.finalize = None;
            let started = Instant::now();
            let result = transform_record_with_base_dir(&partial_rule, record, context, base_dir);
            let duration_us = started.elapsed().as_micros() as u64;
            step_outputs.push((result, duration_us));
        }

        let mut prev_output = JsonValue::Object(JsonMap::new());
        let mut halted = false;
        let mut prev_elapsed = 0u64;
        for (index, step) in steps.iter().enumerate() {
            let label = step
                .name
                .clone()
                .unwrap_or_else(|| format!("step-{}", index + 1));
            let kind = if step.branch.is_some() {
                "branch"
            } else if step.record_when.is_some() {
                "record_when"
            } else if step.asserts.is_some() {
                "asserts"
            } else if step.mappings.is_some() {
                "mappings"
            } else {
                "step"
            };

            let step_input = prev_output.clone();
            let mut status = "ok".to_string();
            let mut output_value: Option<JsonValue> = None;
            let mut error: Option<JsonValue> = None;
            let mut child_trace: Option<JsonValue> = None;
            let mut meta = JsonMap::new();

            let step_active = !halted;
            let (step_result, elapsed_total) = match step_outputs.get(index) {
                Some((result, elapsed)) => (result.clone(), *elapsed),
                None => (
                    Err(TransformError::new(
                        TransformErrorKind::InvalidInput,
                        "missing step output",
                    )),
                    0,
                ),
            };
            let step_duration_us = elapsed_total.saturating_sub(prev_elapsed);
            prev_elapsed = elapsed_total;

            if halted {
                status = "skipped".to_string();
            } else {
                match step_result {
                    Ok(Some(out)) => {
                        prev_output = out.clone();
                        output_value = Some(out.clone());
                    }
                    Ok(None) => {
                        status = "skipped".to_string();
                        output_value = Some(JsonValue::Null);
                        halted = true;
                    }
                    Err(err) => {
                        status = "error".to_string();
                        error = Some(transform_error_to_trace(&err));
                        halted = true;
                    }
                }
            }

            if step_active && status != "error" {
                if let Some(expr) = step.record_when.as_ref() {
                    match eval_trace_condition(
                        expr,
                        record,
                        context,
                        &step_input,
                        "record_when",
                        rule.version,
                    ) {
                        Ok(flag) => {
                            meta.insert("record_when".to_string(), JsonValue::Bool(flag));
                        }
                        Err(err) => {
                            status = "error".to_string();
                            error = Some(transform_error_to_trace(&err));
                            halted = true;
                        }
                    }
                }
            }

            if step_active && status != "error" {
                if let Some(asserts) = step.asserts.as_ref() {
                    let mut asserts_ok = true;
                    for (assert_index, assert) in asserts.iter().enumerate() {
                        let assert_path =
                            format!("steps[{}].asserts[{}].when", index, assert_index);
                        match eval_trace_condition(
                            &assert.when,
                            record,
                            context,
                            &step_input,
                            &assert_path,
                            rule.version,
                        ) {
                            Ok(true) => {}
                            Ok(false) => {
                                asserts_ok = false;
                                let err = TransformError::new(
                                    TransformErrorKind::AssertionFailed,
                                    format!(
                                        "assert failed: {}: {}",
                                        assert.error.code, assert.error.message
                                    ),
                                )
                                .with_path(format!("steps[{}].asserts[{}]", index, assert_index));
                                status = "error".to_string();
                                error = Some(transform_error_to_trace(&err));
                                halted = true;
                                break;
                            }
                            Err(err) => {
                                asserts_ok = false;
                                status = "error".to_string();
                                error = Some(transform_error_to_trace(&err));
                                halted = true;
                                break;
                            }
                        }
                    }
                    meta.insert("asserts_ok".to_string(), JsonValue::Bool(asserts_ok));
                }
            }
            if step.asserts.is_some() && !meta.contains_key("asserts_ok") {
                meta.insert("asserts_ok".to_string(), JsonValue::Bool(false));
            }

            if step_active && status != "error" {
                if let Some(branch) = step.branch.as_ref() {
                    let mut refs = Vec::new();
                    let mut labels = Vec::new();
                    let then_ref = rule_ref_from_rule(base_dir, &branch.then);
                    refs.push(then_ref.clone());
                    labels.push("branch: then".to_string());
                    let else_ref = branch
                        .r#else
                        .as_ref()
                        .map(|other| rule_ref_from_rule(base_dir, other));
                    if let Some(other_ref) = else_ref.as_ref() {
                        refs.push(other_ref.clone());
                        labels.push("branch: else".to_string());
                    }

                    let branch_taken = match eval_trace_condition(
                        &branch.when,
                        record,
                        context,
                        &step_input,
                        "branch.when",
                        rule.version,
                    ) {
                        Ok(true) => "then",
                        Ok(false) => {
                            if branch.r#else.is_some() {
                                "else"
                            } else {
                                "none"
                            }
                        }
                        Err(err) => {
                            status = "error".to_string();
                            error = Some(transform_error_to_trace(&err));
                            halted = true;
                            "none"
                        }
                    };
                    meta.insert(
                        "branch_taken".to_string(),
                        JsonValue::String(branch_taken.to_string()),
                    );
                    meta.insert(
                        "rule_refs".to_string(),
                        JsonValue::Array(refs.iter().cloned().map(JsonValue::String).collect()),
                    );
                    meta.insert(
                        "rule_ref_labels".to_string(),
                        JsonValue::Array(labels.iter().cloned().map(JsonValue::String).collect()),
                    );
                    if branch.return_ && branch_taken != "none" {
                        halted = true;
                    }

                    let taken_ref = match branch_taken {
                        "then" => Some((branch.then.as_str(), then_ref)),
                        "else" => branch
                            .r#else
                            .as_deref()
                            .and_then(|path| else_ref.map(|label| (path, label))),
                        _ => None,
                    };
                    if let Some((target_path, ref_label)) = taken_ref {
                        meta.insert("rule_ref".to_string(), JsonValue::String(ref_label.clone()));
                        meta.insert(
                            "rule_ref_label".to_string(),
                            JsonValue::String(format!("branch: {}", branch_taken)),
                        );
                        let resolved = resolve_rule_path(base_dir, target_path);
                        if let Ok(RuleKind::Normal(loaded)) = load_rule_kind(&resolved) {
                            let rule_source = std::fs::read_to_string(&resolved)
                                .ok()
                                .and_then(|source| yaml_source_to_json(&source))
                                .unwrap_or_else(|| json!({}));
                            let child_rule_trace = build_rule_nodes_from_rule(
                                &loaded.rule,
                                &step_input,
                                context,
                                &loaded.base_dir,
                            );
                            let child_duration_us = child_rule_trace.duration_us;
                            let child_output = transform_record_with_base_dir(
                                &loaded.rule,
                                &step_input,
                                context,
                                &loaded.base_dir,
                            )
                            .ok()
                            .and_then(|value| value)
                            .unwrap_or_else(empty_object);
                            let trace_output = child_rule_trace
                                .pre_finalize_output
                                .clone()
                                .unwrap_or_else(|| child_output.clone());
                            child_trace = Some(build_rule_trace(
                                "normal",
                                rule_display_name(&resolved),
                                rule_ref_from_path(base_dir, &resolved),
                                loaded.rule.version,
                                rule_source,
                                step_input.clone(),
                                trace_output,
                                child_rule_trace.nodes,
                                child_rule_trace.finalize,
                                child_duration_us,
                                "ok",
                            ));
                        }
                    }
                }
            }

            let children = if status == "ok" {
                if let Some(mappings) = step.mappings.as_deref() {
                    let mut mapping_out = step_input.clone();
                    build_mapping_ops_with_values(
                        mappings,
                        record,
                        context,
                        &mut mapping_out,
                        rule.version,
                        index,
                    )
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            let mut node = json!({
                "id": format!("step-{}", index),
                "kind": kind,
                "label": label,
                "status": status,
                "input": step_input,
                "output": output_value,
                "duration_us": step_duration_us,
            });
            if let Some(err) = error {
                if let Some(obj) = node.as_object_mut() {
                    obj.insert("error".to_string(), err);
                }
            }
            if let Some(trace) = child_trace {
                if let Some(obj) = node.as_object_mut() {
                    obj.insert("child_trace".to_string(), trace);
                }
            }
            if !meta.is_empty() {
                if let Some(obj) = node.as_object_mut() {
                    obj.insert("meta".to_string(), JsonValue::Object(meta));
                }
            }
            if !children.is_empty() {
                if let Some(obj) = node.as_object_mut() {
                    obj.insert("children".to_string(), JsonValue::Array(children));
                }
            }
            nodes.push(node);
        }
    } else {
        let started = Instant::now();
        let mut out = JsonValue::Object(JsonMap::new());
        let children = build_mapping_ops_with_values(
            &rule.mappings,
            record,
            context,
            &mut out,
            rule.version,
            0,
        );
        let duration_us = started.elapsed().as_micros() as u64;
        let mut node = json!({
            "id": "step-0",
            "kind": "mapping",
            "label": "mappings",
            "status": "ok",
            "input": record,
            "output": out,
            "duration_us": duration_us,
        });
        if !children.is_empty() {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("children".to_string(), JsonValue::Array(children));
            }
        }
        nodes.push(node);
    }

    if let Some(finalize) = &rule.finalize {
        let mut base_rule = rule.clone();
        base_rule.finalize = None;
        let base_started = Instant::now();
        let pre_finalize = transform_record_with_base_dir(&base_rule, record, context, base_dir)
            .ok()
            .and_then(|value| value);
        let base_duration_us = base_started.elapsed().as_micros() as u64;
        pre_finalize_output = pre_finalize.clone();
        let finalize_input = match pre_finalize {
            Some(value) => JsonValue::Array(vec![value]),
            None => JsonValue::Array(Vec::new()),
        };
        let finalize_started = Instant::now();
        let finalize_result = transform_record_with_base_dir(rule, record, context, base_dir);
        let total_duration_us = finalize_started.elapsed().as_micros() as u64;
        let finalize_duration_us = total_duration_us.saturating_sub(base_duration_us);
        let mut finalize_status = "ok";
        let mut finalize_output: Option<JsonValue> = None;
        let mut finalize_error: Option<JsonValue> = None;
        match finalize_result {
            Ok(Some(value)) => {
                finalize_output = Some(value);
            }
            Ok(None) => {
                finalize_output = Some(JsonValue::Null);
            }
            Err(err) => {
                finalize_status = "error";
                finalize_error = Some(transform_error_to_trace(&err));
            }
        }
        let mut children = Vec::new();
        if let Some(filter) = &finalize.filter {
            children.push(json!({
                "id": "op-filter",
                "kind": "op",
                "label": "filter",
                "status": "ok",
                "meta": { "op": "filter" },
                "args": { "expr": expr_to_json_value(filter) }
            }));
        }
        if let Some(sort) = &finalize.sort {
            children.push(json!({
                "id": "op-sort",
                "kind": "op",
                "label": "sort",
                "status": "ok",
                "meta": { "op": "sort" },
                "args": { "by": sort.by, "order": sort.order }
            }));
        }
        if let Some(limit) = finalize.limit {
            children.push(json!({
                "id": "op-limit",
                "kind": "op",
                "label": "limit",
                "status": "ok",
                "meta": { "op": "limit" },
                "args": { "limit": limit }
            }));
        }
        if let Some(offset) = finalize.offset {
            children.push(json!({
                "id": "op-offset",
                "kind": "op",
                "label": "offset",
                "status": "ok",
                "meta": { "op": "offset" },
                "args": { "offset": offset }
            }));
        }
        if let Some(wrap) = &finalize.wrap {
            children.push(json!({
                "id": "op-wrap",
                "kind": "op",
                "label": "wrap",
                "status": "ok",
                "meta": { "op": "wrap" },
                "args": { "wrap": wrap }
            }));
        }

        let mut finalize = json!({
            "status": finalize_status,
            "input": finalize_input,
            "output": finalize_output,
            "duration_us": finalize_duration_us,
            "nodes": children,
        });
        if let Some(err) = finalize_error {
            if let Some(obj) = finalize.as_object_mut() {
                obj.insert("error".to_string(), err);
            }
        }
        finalize_trace = Some(finalize);
    }

    let duration_us = sum_node_duration_us(&nodes);

    RuleTraceNodes {
        nodes,
        finalize: finalize_trace,
        pre_finalize_output,
        duration_us,
    }
}

fn sum_node_duration_us(nodes: &[JsonValue]) -> u64 {
    nodes
        .iter()
        .filter_map(|node| node.get("duration_us").and_then(|value| value.as_u64()))
        .sum()
}

fn transform_error_to_trace(err: &TransformError) -> JsonValue {
    json!({
        "code": format!("{:?}", err.kind),
        "message": err.message,
        "path": err.path,
    })
}

fn eval_trace_condition(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    rule_version: u8,
) -> Result<bool, TransformError> {
    if rule_version >= 2 {
        if let Some(raw_value) = expr_to_json_for_v2_condition(expr) {
            if let Ok(condition) = parse_v2_condition(&raw_value) {
                let ctx = V2EvalContext::new();
                return eval_v2_condition(&condition, record, context, out, path, &ctx);
            }
            if let Ok(v2_expr) = parse_v2_expr(&raw_value) {
                let ctx = V2EvalContext::new();
                let value = eval_v2_expr(&v2_expr, record, context, out, path, &ctx)?;
                return match value {
                    EvalValue::Missing => Ok(false),
                    EvalValue::Value(JsonValue::Bool(flag)) => Ok(flag),
                    EvalValue::Value(_) => Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "when/record_when must evaluate to boolean",
                    )
                    .with_path(path)),
                };
            }
        }
        if let Some(raw_value) = expr_to_json_for_v2_pipe(expr) {
            let v2_expr = parse_v2_expr(&raw_value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 condition: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new();
            let value = eval_v2_expr(&v2_expr, record, context, out, path, &ctx)?;
            return match value {
                EvalValue::Missing => Ok(false),
                EvalValue::Value(JsonValue::Bool(flag)) => Ok(flag),
                EvalValue::Value(_) => Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "when/record_when must evaluate to boolean",
                )
                .with_path(path)),
            };
        }
    }

    Err(TransformError::new(
        TransformErrorKind::ExprError,
        "when/record_when must evaluate to boolean",
    )
    .with_path(path))
}

fn build_network_nodes_with_timing(
    rule: &CompiledNetworkRule,
    timing: &NetworkExecution,
) -> Vec<JsonValue> {
    let mut children = Vec::new();
    let mut request_args = JsonMap::new();
    request_args.insert(
        "method".to_string(),
        JsonValue::String(rule.request.method.to_string()),
    );
    request_args.insert(
        "url".to_string(),
        JsonValue::String(format!("{:?}", rule.request.url)),
    );
    if !rule.request.headers.is_empty() {
        let mut headers = JsonMap::new();
        for (key, expr) in &rule.request.headers {
            headers.insert(key.to_string(), JsonValue::String(format!("{:?}", expr)));
        }
        request_args.insert("headers".to_string(), JsonValue::Object(headers));
    }
    children.push(json!({
        "id": "op-request",
        "kind": "op",
        "label": "request",
        "status": "ok",
        "duration_us": timing.request_us,
        "meta": { "op": "request" },
        "args": JsonValue::Object(request_args)
    }));

    if let Some(body) = &rule.body {
        children.push(json!({
            "id": "op-body",
            "kind": "op",
            "label": "body",
            "status": "ok",
            "meta": { "op": "body" },
            "args": { "expr": format!("{:?}", body) }
        }));
    }
    if let Some(body_map) = &rule.body_map {
        let mut out = JsonValue::Object(JsonMap::new());
        let empty = JsonValue::Object(JsonMap::new());
        let ops = build_mapping_ops_with_values(body_map, &empty, None, &mut out, 2, 0);
        children.extend(ops);
    }
    if rule.body_rule.is_some() {
        children.push(json!({
            "id": "op-body-rule",
            "kind": "op",
            "label": "body_rule",
            "status": "ok",
            "meta": { "op": "body_rule" }
        }));
    }
    if let Some(select) = &rule.select {
        children.push(json!({
            "id": "op-select",
            "kind": "op",
            "label": "select",
            "status": "ok",
            "meta": { "op": "select" },
            "args": { "path": select }
        }));
    }
    if let Some(retry) = &rule.retry {
        children.push(json!({
            "id": "op-retry",
            "kind": "op",
            "label": "retry",
            "status": "ok",
            "meta": { "op": "retry" },
            "args": {
                "max": retry.max,
                "backoff": format!("{:?}", retry.backoff),
                "initial_delay_ms": retry.initial_delay.as_millis()
            }
        }));
    }

    let mut node = json!({
        "id": "step-0",
        "kind": "network",
        "label": "request",
        "status": "ok",
        "duration_us": timing.total_us,
    });
    if let Some(rule_ref) = rule.body_rule_ref.as_ref() {
        if let Some(obj) = node.as_object_mut() {
            obj.insert(
                "meta".to_string(),
                json!({
                    "rule_ref": rule_ref,
                    "rule_ref_label": "body_rule"
                }),
            );
        }
    }
    if let Some(trace) = timing.body_rule_trace.as_ref() {
        if let Some(obj) = node.as_object_mut() {
            obj.insert("child_trace".to_string(), trace.clone());
        }
    }
    if let Some(obj) = node.as_object_mut() {
        obj.insert("children".to_string(), JsonValue::Array(children));
    }
    vec![node]
}

fn build_mapping_ops_with_values(
    mappings: &[Mapping],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &mut JsonValue,
    rule_version: u8,
    step_index: usize,
) -> Vec<JsonValue> {
    let mut ops = Vec::new();
    for (index, mapping) in mappings.iter().enumerate() {
        let op_started = Instant::now();
        let mut args = JsonMap::new();
        args.insert(
            "target".to_string(),
            JsonValue::String(mapping.target.clone()),
        );
        if let Some(source) = &mapping.source {
            args.insert("source".to_string(), JsonValue::String(source.clone()));
        }
        if let Some(value) = &mapping.value {
            args.insert("value".to_string(), value.clone());
        }
        if let Some(expr) = &mapping.expr {
            args.insert("expr".to_string(), expr_to_json_value(expr));
        }
        if let Some(when) = &mapping.when {
            args.insert("when".to_string(), expr_to_json_value(when));
        }
        if let Some(value_type) = &mapping.value_type {
            args.insert("type".to_string(), JsonValue::String(value_type.clone()));
        }
        if mapping.required {
            args.insert("required".to_string(), JsonValue::Bool(true));
        }
        if let Some(default) = &mapping.default {
            args.insert("default".to_string(), default.clone());
        }

        let mut input_value = None;
        let mut output_value = None;
        let mut pipe_value = None;
        let mut pipe_steps: Option<Vec<JsonValue>> = None;
        if let Some(expr) = &mapping.expr {
            if rule_version >= 2 {
                if let Some(raw) = expr_to_json_for_v2_pipe(expr) {
                    pipe_value = Some(raw.clone());
                    if let Ok(pipe) = parse_v2_pipe_from_value(&raw) {
                        let ctx = V2EvalContext::new();
                        input_value = eval_v2_start_value(&pipe.start, record, context, out, &ctx);
                        output_value = eval_v2_pipe_value(&pipe, record, context, out, &ctx);
                        pipe_steps = Some(build_pipe_steps(&pipe, record, context, out, &ctx));
                    }
                }
            }
        } else if let Some(source) = &mapping.source {
            input_value = resolve_source_value(source, record, context, out);
            output_value = input_value.clone();
            pipe_steps = Some(vec![json!({
                "index": 0,
                "label": "source",
                "input": input_value,
                "output": output_value
            })]);
        } else if let Some(value) = &mapping.value {
            input_value = Some(value.clone());
            output_value = Some(value.clone());
            pipe_steps = Some(vec![json!({
                "index": 0,
                "label": "value",
                "input": input_value,
                "output": output_value
            })]);
        }

        if let Some(value) = output_value.clone() {
            let _ = set_path_value(out, &mapping.target, value);
        }

        let duration_us = op_started.elapsed().as_micros() as u64;
        ops.push(json!({
            "id": format!("op-{}-{}", step_index, index),
            "kind": "op",
            "label": mapping.target,
            "status": "ok",
            "input": input_value,
            "pipe_value": pipe_value,
            "pipe_steps": pipe_steps,
            "args": JsonValue::Object(args),
            "output": output_value,
            "duration_us": duration_us,
            "meta": { "op": "mapping" }
        }));
    }
    ops
}

fn expr_to_json_for_v2_pipe(expr: &Expr) -> Option<JsonValue> {
    match expr {
        Expr::Literal(JsonValue::Array(arr)) => Some(JsonValue::Array(arr.clone())),
        Expr::Literal(JsonValue::String(value)) => {
            if is_v2_ref(value) || is_pipe_value(value) || is_literal_escape(value) {
                Some(JsonValue::String(value.clone()))
            } else {
                None
            }
        }
        Expr::Ref(expr_ref)
            if expr_ref.ref_path.starts_with('@') || is_literal_escape(&expr_ref.ref_path) =>
        {
            Some(JsonValue::Array(vec![JsonValue::String(
                expr_ref.ref_path.clone(),
            )]))
        }
        Expr::Chain(chain) => {
            if let Some(first) = chain.chain.first() {
                if let Expr::Ref(reference) = first {
                    if reference.ref_path.starts_with('@') {
                        let items: Vec<JsonValue> =
                            chain.chain.iter().map(expr_to_json_value).collect();
                        return Some(JsonValue::Array(items));
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn expr_to_json_for_v2_condition(expr: &Expr) -> Option<JsonValue> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        Expr::Ref(reference)
            if reference.ref_path.starts_with('@') || is_literal_escape(&reference.ref_path) =>
        {
            Some(JsonValue::String(reference.ref_path.clone()))
        }
        Expr::Chain(chain) => {
            if let Some(first) = chain.chain.first() {
                if let Expr::Ref(reference) = first {
                    if reference.ref_path.starts_with('@') {
                        let items: Vec<JsonValue> = chain
                            .chain
                            .iter()
                            .map(expr_to_json_value_for_condition)
                            .collect();
                        return Some(JsonValue::Array(items));
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn expr_to_json_value_for_condition(expr: &Expr) -> JsonValue {
    match expr {
        Expr::Ref(reference) => JsonValue::String(reference.ref_path.clone()),
        Expr::Literal(value) => value.clone(),
        Expr::Op(op) => {
            let args: Vec<JsonValue> = op
                .args
                .iter()
                .map(expr_to_json_value_for_condition)
                .collect();
            let mut obj = JsonMap::new();
            obj.insert(op.op.clone(), JsonValue::Array(args));
            JsonValue::Object(obj)
        }
        Expr::Chain(chain) => {
            let items: Vec<JsonValue> = chain
                .chain
                .iter()
                .map(expr_to_json_value_for_condition)
                .collect();
            JsonValue::Array(items)
        }
    }
}

fn build_pipe_steps(
    pipe: &rulemorph::v2_model::V2Pipe,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    ctx: &V2EvalContext,
) -> Vec<JsonValue> {
    let mut steps = Vec::new();
    let start_value = eval_v2_start(&pipe.start, record, context, out, "trace", ctx).ok();
    let start_output = start_value.clone().and_then(eval_value_to_json);
    steps.push(json!({
        "index": 0,
        "label": v2_start_label(&pipe.start),
        "input": JsonValue::Null,
        "output": start_output
    }));

    let mut current = match start_value {
        Some(value) => value,
        None => return steps,
    };
    let mut current_ctx = ctx.clone();

    for (index, step) in pipe.steps.iter().enumerate() {
        let step_input = eval_value_to_json(current.clone());
        current_ctx = current_ctx.clone().with_pipe_value(current.clone());
        let step_path = format!("trace[{}]", index + 1);
        match step {
            V2Step::Op(op_step) => {
                if let Ok(next) = eval_v2_op_step(
                    op_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                ) {
                    current = next;
                }
            }
            V2Step::Let(let_step) => {
                if let Ok(next_ctx) = eval_v2_let_step(
                    let_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                ) {
                    current_ctx = next_ctx;
                }
            }
            V2Step::If(if_step) => {
                if let Ok(next) = eval_v2_if_step(
                    if_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                ) {
                    current = next;
                }
            }
            V2Step::Map(map_step) => {
                if let Ok(next) = eval_v2_map_step(
                    map_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                ) {
                    current = next;
                }
            }
            V2Step::Ref(v2_ref) => {
                if let Ok(next) =
                    eval_v2_ref(v2_ref, record, context, out, &step_path, &current_ctx)
                {
                    current = next;
                }
            }
        }

        steps.push(json!({
            "index": index + 1,
            "label": v2_step_label(step),
            "input": step_input,
            "output": eval_value_to_json(current.clone())
        }));
    }

    steps
}

fn v2_start_label(start: &V2Start) -> String {
    match start {
        V2Start::Ref(reference) => v2_ref_label(reference),
        V2Start::PipeValue => "$".to_string(),
        V2Start::Literal(value) => value.to_string(),
        V2Start::V1Expr(_) => "v1_expr".to_string(),
    }
}

fn v2_step_label(step: &V2Step) -> String {
    match step {
        V2Step::Op(op) => op.op.clone(),
        V2Step::Let(let_step) => format!(
            "let {}",
            let_step
                .bindings
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        V2Step::If(_) => "if".to_string(),
        V2Step::Map(_) => "map".to_string(),
        V2Step::Ref(reference) => v2_ref_label(reference),
    }
}

fn v2_ref_label(reference: &V2Ref) -> String {
    match reference {
        V2Ref::Input(path) => format!("@input.{}", path),
        V2Ref::Context(path) => format!("@context.{}", path),
        V2Ref::Out(path) => format!("@out.{}", path),
        V2Ref::Item(path) => format!("@item.{}", path),
        V2Ref::Acc(path) => format!("@acc.{}", path),
        V2Ref::Local(name) => format!("@{}", name),
    }
}

fn eval_v2_start_value(
    start: &rulemorph::v2_model::V2Start,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    ctx: &V2EvalContext,
) -> Option<JsonValue> {
    eval_v2_start(start, record, context, out, "trace", ctx)
        .ok()
        .and_then(eval_value_to_json)
}

fn eval_v2_pipe_value(
    pipe: &rulemorph::v2_model::V2Pipe,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    ctx: &V2EvalContext,
) -> Option<JsonValue> {
    eval_v2_pipe(pipe, record, context, out, "trace", ctx)
        .ok()
        .and_then(eval_value_to_json)
}

fn eval_value_to_json(value: EvalValue) -> Option<JsonValue> {
    match value {
        EvalValue::Missing => None,
        EvalValue::Value(value) => Some(value),
    }
}

fn resolve_source_value(
    source: &str,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
) -> Option<JsonValue> {
    let trimmed = source.strip_prefix('@').unwrap_or(source);
    let (prefix, path) = trimmed.split_once('.').unwrap_or(("input", trimmed));
    if path.is_empty() {
        return None;
    }
    let target = match prefix {
        "input" => Some(record),
        "context" => context,
        "out" => Some(out),
        _ => Some(record),
    }?;
    let tokens = parse_path(path).ok()?;
    get_path(target, &tokens).cloned()
}

fn set_path_value(root: &mut JsonValue, path: &str, value: JsonValue) -> Result<(), ()> {
    let tokens = parse_path(path).map_err(|_| ())?;
    if tokens.is_empty() {
        return Err(());
    }
    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let is_last = index == tokens.len() - 1;
        let key = match token {
            PathToken::Key(key) => key,
            PathToken::Index(_) => return Err(()),
        };

        if is_last {
            match current {
                JsonValue::Object(map) => {
                    map.insert(key.to_string(), value);
                }
                _ => {
                    let mut map = JsonMap::new();
                    map.insert(key.to_string(), value);
                    *current = JsonValue::Object(map);
                }
            }
            return Ok(());
        }

        let next = match current {
            JsonValue::Object(map) => map
                .entry(key.to_string())
                .or_insert_with(|| JsonValue::Object(JsonMap::new())),
            _ => {
                *current = JsonValue::Object(JsonMap::new());
                if let JsonValue::Object(map) = current {
                    map.entry(key.to_string())
                        .or_insert_with(|| JsonValue::Object(JsonMap::new()))
                } else {
                    return Err(());
                }
            }
        };
        current = next;
    }
    Err(())
}

fn expr_to_json_value(expr: &Expr) -> JsonValue {
    match expr {
        Expr::Ref(reference) => json!({ "ref": reference.ref_path }),
        Expr::Op(op) => {
            let args: Vec<JsonValue> = op.args.iter().map(expr_to_json_value).collect();
            json!({ "op": op.op, "args": args })
        }
        Expr::Chain(chain) => {
            let items: Vec<JsonValue> = chain.chain.iter().map(expr_to_json_value).collect();
            JsonValue::Array(items)
        }
        Expr::Literal(value) => value.clone(),
    }
}

enum RuleKind {
    Normal(LoadedRule),
    Network(CompiledNetworkRule),
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use rulemorph_trace::TraceStore;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn endpoint_path_matches_and_captures() {
        let path = EndpointPath::parse("/api/traces/{id}").unwrap();
        assert!(path.matches("/api/traces/abc"));
        let params = path.capture("/api/traces/abc");
        assert_eq!(params.get("id"), Some(&"abc".to_string()));
    }

    #[test]
    fn internal_hosts_match_loopback_aliases() {
        assert!(internal_hosts_match(Some("127.0.0.1"), Some("localhost")));
        assert!(internal_hosts_match(Some("localhost"), Some("::1")));
        assert!(!internal_hosts_match(
            Some("127.0.0.1"),
            Some("example.com")
        ));
    }

    #[test]
    fn engine_config_allows_loopback_aliases_for_internal_base() {
        let config = EngineConfig::new(
            "http://localhost:8080".to_string(),
            std::path::PathBuf::from(".data"),
        );

        assert!(
            config
                .ssrf_private_allowlist
                .contains(&"localhost".to_string())
        );
        assert!(
            config
                .ssrf_private_allowlist
                .contains(&"127.0.0.1".to_string())
        );
        assert!(config.ssrf_private_allowlist.contains(&"::1".to_string()));
    }

    #[test]
    fn zip_copy_stops_after_file_limit() {
        let mut input = std::io::Cursor::new(vec![b'x'; 12]);
        let mut output = Vec::new();
        let copied = copy_zip_entry_bounded(&mut input, &mut output, 8).expect("copy");
        assert_eq!(copied, 12);
        assert!(output.len() <= 8);
    }

    #[test]
    fn zip_extract_rejects_too_many_entries() {
        let temp = tempfile::tempdir().expect("tempdir");
        let zip_path = temp.path().join("bundle.zip");
        let file = File::create(&zip_path).expect("create zip");
        let mut zip_writer = zip::ZipWriter::new(file);
        let options =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        for index in 0..=MULTIPART_IMPORT_MAX_ENTRIES {
            zip_writer
                .start_file(format!("rules/{index}.yaml"), options)
                .expect("start file");
        }
        zip_writer.finish().expect("finish zip");

        let err = extract_zip(&zip_path, temp.path().join("out").as_path())
            .expect_err("zip should be rejected");
        assert!(err.contains("too many entries"));
    }

    #[test]
    fn compile_retry_defaults_to_none() {
        let retry = compile_retry(None).unwrap();
        assert!(retry.is_none());
    }

    #[test]
    fn build_headers_rejects_host_header() {
        let mut headers = HashMap::new();
        let expr = parse_v2_expr(&json!("example.com")).expect("parse expr");
        headers.insert("Host".to_string(), expr);
        let err = build_headers(&headers, &json!({}), None).expect_err("expected error");
        assert_eq!(err.kind, EndpointErrorKind::Invalid);
        assert!(err.message.contains("disallowed header"));
    }

    #[test]
    fn ssrf_audit_log_populates_fields() {
        let rule = CompiledNetworkRule {
            request: CompiledNetworkRequest {
                method: Method::GET,
                url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
                headers: HashMap::new(),
            },
            timeout: std::time::Duration::from_secs(1),
            select: None,
            body: None,
            body_map: None,
            body_rule: None,
            body_rule_ref: None,
            rule_ref: Some("rules/network.yaml".to_string()),
            catch: None,
            retry: None,
            internal_auth: false,
            base_dir: PathBuf::from("."),
        };
        let context = RequestContext {
            tenant_id: Some("tenant-1".to_string()),
            internal_api_key: None,
        };
        let log = build_ssrf_audit_log(&rule, "https://example.com", "blocked", Some(&context));
        assert_eq!(log.tenant_id, "tenant-1");
        assert_eq!(log.rule_ref, "rules/network.yaml");
        assert_eq!(log.method, Method::GET);
        assert_eq!(log.url, "https://example.com/");
        assert_eq!(log.reason, "blocked");
    }

    #[test]
    fn ssrf_audit_log_defaults_to_unknown() {
        let rule = CompiledNetworkRule {
            request: CompiledNetworkRequest {
                method: Method::POST,
                url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
                headers: HashMap::new(),
            },
            timeout: std::time::Duration::from_secs(1),
            select: None,
            body: None,
            body_map: None,
            body_rule: None,
            body_rule_ref: None,
            rule_ref: None,
            catch: None,
            retry: None,
            internal_auth: false,
            base_dir: PathBuf::from("."),
        };
        let log = build_ssrf_audit_log(&rule, "https://example.com", "blocked", None);
        assert_eq!(log.tenant_id, "unknown");
        assert_eq!(log.rule_ref, "unknown");
        assert_eq!(log.method, Method::POST);
    }

    #[test]
    fn ssrf_audit_log_redacts_query() {
        let rule = CompiledNetworkRule {
            request: CompiledNetworkRequest {
                method: Method::GET,
                url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
                headers: HashMap::new(),
            },
            timeout: std::time::Duration::from_secs(1),
            select: None,
            body: None,
            body_map: None,
            body_rule: None,
            body_rule_ref: None,
            rule_ref: None,
            catch: None,
            retry: None,
            internal_auth: false,
            base_dir: PathBuf::from("."),
        };
        let log =
            build_ssrf_audit_log(&rule, "https://example.com/path?token=abc", "blocked", None);
        assert_eq!(log.url, "https://example.com/path");
    }

    #[test]
    fn rule_ref_from_path_avoids_double_rules_prefix() {
        let base_dir = PathBuf::from("/tmp/rules");
        let path = base_dir.join("rules").join("endpoint.yaml");
        let rule_ref = rule_ref_from_path(&base_dir, &path);
        assert_eq!(rule_ref, "rules/endpoint.yaml");
    }

    #[test]
    fn eval_expr_string_rejects_non_string() {
        let expr = parse_v2_expr(&json!(123)).expect("parse expr");
        let input = json!({});
        let err = eval_expr_string(&expr, &input, None).expect_err("expected error");
        assert_eq!(err.kind, EndpointErrorKind::Invalid);
        assert!(err.message.contains("expected string"));
    }

    #[test]
    fn endpoint_error_trace_uses_rule_ref_for_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
"#,
        )
        .expect("write endpoint");
        std::fs::create_dir_all(rules_dir.join("rules")).expect("create rules dir");
        std::fs::write(
            rules_dir.join("rules/ok.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "output.ok"
    value: true
"#,
        )
        .expect("write rule");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://127.0.0.1:8080".to_string(), rules_dir.join(".data"))
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let resolved = rules_dir.join("rules/ok.yaml");
        let err = EndpointError::invalid("boom").with_path(resolved.clone());
        let trace = engine.endpoint_error_to_trace(&err);
        let path = trace
            .get("path")
            .and_then(|value| value.as_str())
            .expect("path");

        let expected = rule_ref_from_path(&engine.endpoint_rule.base_dir, &resolved);
        assert_eq!(path, expected);
        assert!(!Path::new(path).is_absolute());
    }

    #[test]
    fn build_trace_emits_top_level_status() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
        )
        .expect("write endpoint.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.join(".data"))
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let trace = engine.build_trace(
            &Method::GET,
            "/api/test",
            json!({"input": true}),
            json!({"output": false}),
            "error".to_string(),
            Some(json!({"message": "boom"})),
            Vec::new(),
            12,
        );
        let status = trace.get("status").and_then(|value| value.as_str());
        assert_eq!(status, Some("error"));
    }

    #[test]
    fn compile_network_rule_rejects_zero_timeout() {
        let raw = NetworkRuleFile {
            version: 2,
            rule_type: "network".to_string(),
            request: NetworkRequest {
                method: "GET".to_string(),
                url: json!("https://example.com"),
                headers: None,
            },
            timeout: "0s".to_string(),
            internal_auth: false,
            select: None,
            body: None,
            body_map: None,
            body_rule: None,
            catch: None,
            retry: None,
        };
        let err = compile_network_rule(raw, Path::new("network.yaml")).expect_err("expected error");
        assert!(err.to_string().contains("timeout must be > 0"));
    }

    #[tokio::test]
    async fn internal_auth_rejected_when_disabled() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
        )
        .expect("write endpoint.yaml");
        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.join(".data"))
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let raw = NetworkRuleFile {
            version: 2,
            rule_type: "network".to_string(),
            request: NetworkRequest {
                method: "GET".to_string(),
                url: json!("https://example.com"),
                headers: None,
            },
            timeout: "1s".to_string(),
            internal_auth: true,
            select: None,
            body: None,
            body_map: None,
            body_rule: None,
            catch: None,
            retry: None,
        };
        let rule = compile_network_rule(raw, Path::new("network.yaml")).expect("compile rule");

        let err = engine
            .send_network_request(&rule, "https://example.com", &HeaderMap::new(), None, None)
            .await
            .expect_err("expected error");
        assert_eq!(err.kind, EndpointErrorKind::Invalid);
        assert!(err.message.contains("internal_auth"));
    }

    #[tokio::test]
    async fn internal_auth_rejects_disallowed_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
        )
        .expect("write endpoint.yaml");
        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost:1234".to_string(), rules_dir.join(".data"))
                .with_internal_auth_enabled(true)
                .with_internal_auth_path_allowlist(vec![
                    "/internal/traces".to_string(),
                    "/internal/traces/".to_string(),
                ])
                .with_internal_api_key("secret".to_string()),
        )
        .expect("load engine");
        let raw = NetworkRuleFile {
            version: 2,
            rule_type: "network".to_string(),
            request: NetworkRequest {
                method: "GET".to_string(),
                url: json!("http://localhost:1234/internal/api-keys"),
                headers: None,
            },
            timeout: "1s".to_string(),
            internal_auth: true,
            select: None,
            body: None,
            body_map: None,
            body_rule: None,
            catch: None,
            retry: None,
        };
        let rule = compile_network_rule(raw, Path::new("network.yaml")).expect("compile rule");

        let err = engine
            .send_network_request(
                &rule,
                "http://localhost:1234/internal/api-keys",
                &HeaderMap::new(),
                None,
                None,
            )
            .await
            .expect_err("expected error");
        assert_eq!(err.kind, EndpointErrorKind::Invalid);
        assert!(err.message.contains("internal_auth path"));
    }

    #[test]
    fn context_internal_api_key_is_injected_on_demand() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
        )
        .expect("write endpoint.yaml");
        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.join(".data"))
                .with_internal_api_key("secret".to_string()),
        )
        .expect("load engine");
        let base_context = engine.build_context_json(None);
        assert!(
            base_context
                .get("config")
                .and_then(|value| value.get("internal_api_key"))
                .is_none()
        );
        let injected = engine.context_with_internal_api_key(&base_context, "secret");
        assert_eq!(
            injected
                .get("config")
                .and_then(|value| value.get("internal_api_key"))
                .and_then(|value| value.as_str()),
            Some("secret")
        );
    }

    #[test]
    fn build_network_body_body_rule_none_omits_body() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_dir.join("body_rule.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
record_when:
  eq: [1, 2]
mappings:
  - target: "name"
    value: "ignored"
"#,
        )
        .expect("write body_rule.yaml");

        let network_path = rules_dir.join("network.yaml");
        std::fs::write(
            &network_path,
            r#"
version: 2
type: network
request:
  method: POST
  url: "https://example.com"
timeout: 1s
body_rule: body_rule.yaml
"#,
        )
        .expect("write network.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let raw: NetworkRuleFile =
            serde_yaml::from_str(&std::fs::read_to_string(&network_path).expect("read network"))
                .expect("parse network");
        let rule = compile_network_rule(raw, &network_path).expect("compile network");

        let body = engine
            .build_network_body(&rule, &json!({}), None)
            .expect("build body");
        assert!(body.is_none());
    }

    #[test]
    fn mapping_ops_include_duration_us() {
        let mappings = vec![Mapping {
            target: "name".to_string(),
            source: None,
            value: Some(json!("hello")),
            expr: None,
            when: None,
            value_type: None,
            required: false,
            default: None,
        }];
        let record = json!({});
        let mut out = json!({});
        let ops = build_mapping_ops_with_values(&mappings, &record, None, &mut out, 2, 0);
        let duration = ops[0].get("duration_us").and_then(|value| value.as_u64());
        assert!(duration.is_some());
    }

    #[tokio::test]
    async fn reply_body_omitted_returns_empty_body() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/empty
    steps: []
    reply:
      status: 204
"#,
        )
        .expect("write endpoint.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/empty")
            .body(axum::body::Body::empty())
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 204);
        assert!(response.headers().get("content-type").is_none());

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn request_body_too_large_writes_trace() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
        )
        .expect("write endpoint.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true)
                .with_max_body_bytes(16),
        )
        .expect("load engine");

        let body = vec![b'a'; 64];
        let request = Request::builder()
            .method("POST")
            .uri("/api/test")
            .body(axum::body::Body::from(body))
            .expect("build request");

        let err = engine
            .handle_request(request)
            .await
            .expect_err("handle request should fail");
        assert!(format!("{err}").contains("payload too large"));

        let store = TraceStore::new(rules_dir.to_path_buf())
            .await
            .expect("trace store");
        let mut items = Vec::new();
        for _ in 0..20 {
            items = store.list().await.expect("trace list");
            if !items.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(!items.is_empty());
        assert!(items.iter().any(|item| item.status == "error"));
    }

    #[tokio::test]
    async fn request_body_read_error_returns_network_error() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
        )
        .expect("write endpoint.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let stream = stream::once(async {
            Err::<axum::body::Bytes, std::io::Error>(std::io::Error::new(
                std::io::ErrorKind::Other,
                "boom",
            ))
        });
        let body = axum::body::Body::from_stream(stream);
        let request = Request::builder()
            .method("POST")
            .uri("/api/test")
            .body(body)
            .expect("build request");

        let err = engine
            .handle_request(request)
            .await
            .expect_err("handle request should fail");
        assert!(format!("{err}").contains("request body read error"));
    }

    #[tokio::test]
    async fn step_catch_inherits_with_params() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/failing_network.yaml
        with:
          fields: ["name"]
        catch:
          default: ./rules/catch.yaml
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("failing_network.yaml"),
            r#"
version: 2
type: network
request:
  method: GET
  url: "http://example.com"
timeout: 1s
body: "@input"
"#,
        )
        .expect("write failing network rule");

        std::fs::write(
            rules_subdir.join("catch.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "params"
    expr: "@context.params"
    required: true
"#,
        )
        .expect("write catch rule");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/test")
            .body(axum::body::Body::empty())
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 200);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "params": { "fields": ["name"] } }));
    }

    #[tokio::test]
    async fn endpoint_duplicate_query_runs_catch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("catch.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "handled"
    value: true
"#,
        )
        .expect("write catch.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/test?dup=1&dup=2")
            .body(axum::body::Body::empty())
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 200);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "handled": true }));
    }

    #[tokio::test]
    async fn endpoint_invalid_json_runs_catch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("catch.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "handled"
    value: true
"#,
        )
        .expect("write catch.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("POST")
            .uri("/api/test")
            .header("content-type", "application/json")
            .body(axum::body::Body::from("{\"bad\":}"))
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 200);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "handled": true }));
    }

    #[tokio::test]
    async fn endpoint_invalid_json_keeps_query_in_catch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("catch.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "query"
    expr: "@input.query"
"#,
        )
        .expect("write catch.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("POST")
            .uri("/api/test?token=abc")
            .header("content-type", "application/json")
            .body(axum::body::Body::from("{\"bad\":}"))
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 200);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "query": { "token": "abc" } }));
    }

    #[tokio::test]
    async fn endpoint_input_mapping_error_runs_catch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    input:
      - target: "user_id"
        source: "input.body.user_id"
        required: true
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("catch.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "handled"
    value: true
"#,
        )
        .expect("write catch.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("POST")
            .uri("/api/test")
            .body(axum::body::Body::empty())
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 200);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "handled": true }));
    }

    #[tokio::test]
    async fn reply_eval_error_runs_catch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: "@input.status"
      body: "@input.body"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("catch.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "status"
    value: 200
  - target: "body"
    value:
      handled: true
"#,
        )
        .expect("write catch.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/test")
            .body(axum::body::Body::empty())
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 200);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "handled": true }));
    }

    #[tokio::test]
    async fn network_url_eval_error_runs_catch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("network.yaml"),
            r#"
version: 2
type: network
request:
  method: GET
  url: "@input.url"
timeout: 1s
catch:
  default: ./catch.yaml
"#,
        )
        .expect("write network.yaml");

        std::fs::write(
            rules_subdir.join("catch.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "handled"
    value: true
"#,
        )
        .expect("write catch.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/test")
            .body(axum::body::Body::empty())
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 200);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "handled": true }));
    }

    #[tokio::test]
    async fn network_body_build_error_runs_catch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("network.yaml"),
            r#"
version: 2
type: network
request:
  method: POST
  url: "https://example.com"
timeout: 1s
body_map:
  - target: "required"
    source: "input.missing"
    required: true
catch:
  default: ./catch.yaml
"#,
        )
        .expect("write network.yaml");

        std::fs::write(
            rules_subdir.join("catch.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "handled"
    value: true
"#,
        )
        .expect("write catch.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("POST")
            .uri("/api/test")
            .body(axum::body::Body::empty())
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 200);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "handled": true }));
    }

    #[tokio::test]
    async fn network_select_error_runs_catch() {
        let app = axum::Router::new().route(
            "/data",
            axum::routing::get(|| async { axum::Json(json!({ "data": { "value": 1 } })) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let host = format!("localhost:{}", addr.port());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = axum::serve(listener, app.into_make_service()).with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        });
        let server_handle = tokio::spawn(async move {
            let _ = server.await;
        });

        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("network.yaml"),
            format!(
                r#"
version: 2
type: network
request:
  method: GET
  url: "http://{}/data"
timeout: 1s
select: "missing.path"
catch:
  default: ./catch.yaml
"#,
                host
            ),
        )
        .expect("write network.yaml");

        std::fs::write(
            rules_subdir.join("catch.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "handled"
    value: true
"#,
        )
        .expect("write catch.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/test")
            .body(axum::body::Body::empty())
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 200);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "handled": true }));

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn multipart_import_body_is_available_to_network_rule() {
        let app = axum::Router::new().route(
            "/internal/import",
            axum::routing::post(
                |headers: HeaderMap, axum::Json(payload): axum::Json<JsonValue>| async move {
                    assert_eq!(
                        headers
                            .get("x-api-key")
                            .and_then(|value| value.to_str().ok()),
                        Some("internal-key")
                    );
                    assert_eq!(
                        headers
                            .get("x-tenant-id")
                            .and_then(|value| value.to_str().ok()),
                        Some("tenant-a")
                    );
                    let bundle_path = payload
                        .get("bundle_path")
                        .and_then(|value| value.as_str())
                        .expect("bundle_path");
                    assert!(Path::new(bundle_path).join("rules/ok.yaml").exists());
                    axum::Json(json!({ "imported": 1, "rules_imported": 1 }))
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let host = format!("localhost:{}", addr.port());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = axum::serve(listener, app.into_make_service()).with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        });
        let server_handle = tokio::spawn(async move {
            let _ = server.await;
        });

        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let network_dir = rules_dir.join("network");
        std::fs::create_dir_all(&network_dir).expect("create network dir");
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/import
    steps:
      - rule: ./network/import_bundle.yaml
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");
        std::fs::write(
            network_dir.join("import_bundle.yaml"),
            r#"
version: 2
type: network
request:
  method: POST
  url:
    - "@context.config.internal_base"
    - concat: ["/internal/import"]
  headers:
    x-tenant-id: "@context.tenant_id"
timeout: 1s
internal_auth: true
body_map:
  - target: "bundle_path"
    source: "input.body.bundle_path"
"#,
        )
        .expect("write import_bundle.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new(format!("http://{}", host), rules_dir.join(".data"))
                .with_internal_auth_enabled(true)
                .with_internal_auth_path_allowlist(vec!["/internal/import".to_string()])
                .with_internal_api_key("internal-key".to_string()),
        )
        .expect("load engine");
        let (boundary, body) = build_multipart_zip_body();
        let mut request = Request::builder()
            .method("POST")
            .uri("/api/import")
            .header(
                axum::http::header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .expect("request");
        request.extensions_mut().insert(RequestContext {
            tenant_id: Some("tenant-a".to_string()),
            internal_api_key: None,
        });
        let response = engine.handle_request(request).await.expect("response");
        assert_eq!(response.status().as_u16(), 200);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "imported": 1, "rules_imported": 1 }));

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn multipart_import_requires_bundle_field() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/import
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");
        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://127.0.0.1:8080".to_string(), rules_dir.join(".data")),
        )
        .expect("load engine");
        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"not_bundle\"\r\n\r\nvalue\r\n--{boundary}--\r\n"
        );
        let request = Request::builder()
            .method("POST")
            .uri("/api/import")
            .header(
                axum::http::header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .expect("request");

        let err = engine
            .handle_request(request)
            .await
            .expect_err("handle request should fail");
        assert!(err.to_string().contains("missing bundle file"));
    }

    #[tokio::test]
    async fn multipart_body_is_only_import_special_case() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/traces
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");
        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://127.0.0.1:8080".to_string(), rules_dir.join(".data")),
        )
        .expect("load engine");
        let (boundary, body) = build_multipart_zip_body();
        let request = Request::builder()
            .method("GET")
            .uri("/api/traces")
            .header(
                axum::http::header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .expect("request");

        let err = engine
            .handle_request(request)
            .await
            .expect_err("multipart should not be parsed on non-import endpoints");
        assert!(!err.to_string().contains("missing bundle file"));
    }

    fn build_multipart_zip_body() -> (String, Vec<u8>) {
        let mut zip_writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let options =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zip_writer
            .start_file("rules/ok.yaml", options)
            .expect("start file");
        zip_writer
            .write_all(
                br#"
version: 2
input:
  format: json
  json: {}
mappings: []
"#,
            )
            .expect("write file");
        let zip_bytes = zip_writer.finish().expect("finish zip").into_inner();
        let boundary = "BOUNDARY".to_string();
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"bundle\"; filename=\"bundle.zip\"\r\n",
        );
        body.extend_from_slice(b"Content-Type: application/zip\r\n\r\n");
        body.extend_from_slice(&zip_bytes);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        (boundary, body)
    }

    #[tokio::test]
    async fn network_response_too_large_returns_error() {
        let payload = "x".repeat(2048);
        let app = axum::Router::new().route(
            "/data",
            axum::routing::get({
                let payload = payload.clone();
                move || {
                    let payload = payload.clone();
                    async move { axum::Json(json!({ "data": payload })) }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let host = format!("localhost:{}", addr.port());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = axum::serve(listener, app.into_make_service()).with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        });
        let server_handle = tokio::spawn(async move {
            let _ = server.await;
        });

        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("network.yaml"),
            format!(
                r#"
version: 2
type: network
request:
  method: GET
  url: "http://{}/data"
timeout: 1s
"#,
                host
            ),
        )
        .expect("write network.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true)
                .with_max_response_bytes(128),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/test")
            .body(axum::body::Body::empty())
            .expect("build request");

        let err = engine
            .handle_request(request)
            .await
            .expect_err("handle request should fail");
        assert!(format!("{err}").contains("payload too large"));

        let store = TraceStore::new(rules_dir.to_path_buf())
            .await
            .expect("trace store");
        let mut items = Vec::new();
        for _ in 0..20 {
            items = store.list().await.expect("trace list");
            if !items.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(!items.is_empty());
        assert!(items.iter().any(|item| item.status == "error"));

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn network_chunked_response_too_large_returns_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let host = format!("localhost:{}", addr.port());

        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let headers = concat!(
                "HTTP/1.1 200 OK\r\n",
                "content-type: application/json\r\n",
                "transfer-encoding: chunked\r\n",
                "\r\n"
            );
            let _ = socket.write_all(headers.as_bytes()).await;

            let chunk1 = "{\"data\":\"";
            let chunk2 = format!("{}\"}}", "x".repeat(64));
            let chunk1_line = format!("{:X}\r\n{}\r\n", chunk1.len(), chunk1);
            let chunk2_line = format!("{:X}\r\n{}\r\n", chunk2.len(), chunk2);
            let _ = socket.write_all(chunk1_line.as_bytes()).await;
            let _ = socket.write_all(chunk2_line.as_bytes()).await;
            let _ = socket.write_all(b"0\r\n\r\n").await;
            let _ = socket.flush().await;
            let _ = socket.shutdown().await;
        });

        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("network.yaml"),
            format!(
                r#"
version: 2
type: network
request:
  method: GET
  url: "http://{}/data"
timeout: 1s
"#,
                host
            ),
        )
        .expect("write network.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true)
                .with_max_response_bytes(32),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/test")
            .body(axum::body::Body::empty())
            .expect("build request");

        let err = engine
            .handle_request(request)
            .await
            .expect_err("handle request should fail");
        assert!(format!("{err}").contains("payload too large"));

        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn network_timeout_on_slow_body_runs_catch() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let host = format!("localhost:{}", addr.port());

        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let body = b"{\"value\":1}";
            let headers = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(headers.as_bytes()).await;
            let _ = socket.flush().await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = socket.write_all(body).await;
            let _ = socket.shutdown().await;
        });

        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("network.yaml"),
            format!(
                r#"
version: 2
type: network
request:
  method: GET
  url: "http://{}/slow"
timeout: 100ms
catch:
  timeout: ./catch.yaml
"#,
                host
            ),
        )
        .expect("write network.yaml");

        std::fs::write(
            rules_subdir.join("catch.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "handled"
    value: true
"#,
        )
        .expect("write catch.yaml");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/test")
            .body(axum::body::Body::empty())
            .expect("build request");

        let response = engine
            .handle_request(request)
            .await
            .expect("handle request");
        assert_eq!(response.status().as_u16(), 200);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
        assert_eq!(body, json!({ "handled": true }));

        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn step_rule_record_when_false_returns_error() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rules_dir = temp.path();
        let rules_subdir = rules_dir.join("rules");
        std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

        std::fs::write(
            rules_dir.join("endpoint.yaml"),
            r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/filter
    steps:
      - rule: ./rules/filter.yaml
    reply:
      status: 200
      body: "@input"
"#,
        )
        .expect("write endpoint.yaml");

        std::fs::write(
            rules_subdir.join("filter.yaml"),
            r#"
version: 2
input:
  format: json
  json: {}
record_when:
  eq: [1, 2]
mappings:
  - target: "ignored"
    value: "nope"
"#,
        )
        .expect("write filter rule");

        let engine = EndpointEngine::load(
            rules_dir.to_path_buf(),
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
                .with_ssrf_allow_private(true),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/filter")
            .body(axum::body::Body::empty())
            .expect("build request");

        let err = engine
            .handle_request(request)
            .await
            .expect_err("expected error");
        assert!(err.to_string().contains("record"));
    }

    #[test]
    fn rule_nodes_include_step_duration_us() {
        let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: name
        value: "hello"
"#;
        let rule = parse_rule_file(yaml).expect("parse rule");
        let record = json!({});
        let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
        let duration = trace.nodes[0]
            .get("duration_us")
            .and_then(|value| value.as_u64());
        assert!(duration.is_some());
    }

    #[test]
    fn network_nodes_include_request_duration_us() {
        let body_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings: []
"#;
        let body_rule = parse_rule_file(body_yaml).expect("parse body rule");
        let rule = CompiledNetworkRule {
            request: CompiledNetworkRequest {
                method: Method::GET,
                url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
                headers: HashMap::new(),
            },
            timeout: Duration::from_secs(1),
            select: None,
            body: None,
            body_map: None,
            body_rule: Some(LoadedRule {
                rule: body_rule,
                base_dir: PathBuf::from("."),
            }),
            body_rule_ref: Some("rules/body.yaml".to_string()),
            rule_ref: None,
            catch: None,
            retry: None,
            internal_auth: false,
            base_dir: PathBuf::from("."),
        };
        let timing = NetworkExecution {
            output: json!({}),
            request_us: 12,
            total_us: 34,
            body_rule_trace: Some(json!({
                "rule": { "path": "rules/body.yaml" },
                "records": []
            })),
        };

        let nodes = build_network_nodes_with_timing(&rule, &timing);
        let duration = nodes[0].get("duration_us").and_then(|value| value.as_u64());
        assert_eq!(duration, Some(34));
        let meta = nodes[0]
            .get("meta")
            .and_then(|value| value.as_object())
            .expect("meta");
        assert_eq!(meta.get("rule_ref"), Some(&json!("rules/body.yaml")));
        assert_eq!(meta.get("rule_ref_label"), Some(&json!("body_rule")));
        let child_trace = nodes[0]
            .get("child_trace")
            .and_then(|value| value.get("rule"))
            .and_then(|value| value.get("path"));
        assert_eq!(child_trace, Some(&json!("rules/body.yaml")));

        let children = nodes[0]
            .get("children")
            .and_then(|value| value.as_array())
            .expect("children");
        assert_eq!(children.len(), 2);
        let request = children[0]
            .get("duration_us")
            .and_then(|value| value.as_u64());
        assert_eq!(request, Some(12));
    }

    #[test]
    #[ignore]
    fn trace_timing_perf_smoke() {
        let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: name
        value: "hello"
  - mappings:
      - target: upper
        expr: ["@out.name", uppercase]
"#;
        let rule = parse_rule_file(yaml).expect("parse rule");
        let record = json!({});
        let iterations = 100u64;
        let started = Instant::now();
        for _ in 0..iterations {
            let _ = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
        }
        let total_us = started.elapsed().as_micros() as u64;
        println!("trace timing avg: {} μs", total_us / iterations);
    }
}
