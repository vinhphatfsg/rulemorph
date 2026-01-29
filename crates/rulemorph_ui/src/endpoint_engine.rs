use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode};
use axum::response::Response;
use chrono::{Datelike, Utc};
use reqwest::Client;
use rulemorph::v2_eval::{
    eval_v2_condition, eval_v2_expr, eval_v2_if_step, eval_v2_let_step, eval_v2_map_step,
    eval_v2_op_step, eval_v2_pipe, eval_v2_ref, eval_v2_start, EvalValue, V2EvalContext,
};
use rulemorph::v2_parser::{
    is_literal_escape, is_pipe_value, is_v2_ref, parse_v2_condition, parse_v2_expr,
    parse_v2_pipe_from_value,
};
use rulemorph::PathToken;
use rulemorph::v2_model::{V2Ref, V2Start, V2Step};
use rulemorph::{
    get_path, parse_path, parse_rule_file, transform_record, transform_record_with_base_dir,
    validate_rule_file_with_source, Expr, Mapping, RuleError, RuleFile, TransformError,
    TransformErrorKind,
};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use tracing::warn;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiMode {
    UiOnly,
    Rules,
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
}

impl EngineConfig {
    pub fn new(internal_base: String, data_dir: PathBuf) -> Self {
        Self {
            internal_base,
            data_dir,
        }
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
    client: Client,
}

struct RuleExecution {
    output: JsonValue,
    child_trace: Option<JsonValue>,
}

struct NetworkExecution {
    output: JsonValue,
    request_us: u64,
    total_us: u64,
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
        let raw_rule_source = serde_json::to_value(raw_source)
            .unwrap_or_else(|_| json!({}));
        let raw: EndpointRuleFile = serde_yaml::from_str(&source)
            .with_context(|| format!("failed to parse {}", endpoint_path.display()))?;
        if raw.version != 2 {
            return Err(anyhow!("endpoint rule version must be 2"));
        }
        if raw.rule_type != "endpoint" {
            return Err(anyhow!("endpoint rule type must be endpoint"));
        }
        let compiled = CompiledEndpointRule::compile(raw.clone(), &endpoint_path)?;
        let client = Client::builder()
            .no_proxy()
            .build()
            .map_err(|err| anyhow!(err.to_string()))?;
        Ok(Self {
            endpoint_rule: compiled,
            raw_rule_source,
            config,
            client,
        })
    }

    pub async fn handle_request(&self, request: Request<axum::body::Body>) -> Result<Response> {
        let started = Instant::now();
        let (parts, body) = request.into_parts();
        let method = parts.method.clone();
        let path = parts.uri.path().to_string();
        let endpoint_match = self
            .endpoint_rule
            .match_endpoint(&method, &path)
            .ok_or_else(|| anyhow!("no endpoint matched"))?;
        let body_bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        let body_value = if body_bytes.is_empty() {
            None
        } else {
            Some(
                serde_json::from_slice::<JsonValue>(&body_bytes)
                    .map_err(|err| anyhow!(err.to_string()))?,
            )
        };

        let endpoint = endpoint_match.endpoint;
        let input = build_input(&parts, &endpoint_match.params, body_value)?;
        let record_input = input.clone();
        let mut current = if let Some(mappings) = &endpoint.input {
            apply_mappings_via_rule(mappings, &input, Some(&self.config_json()))?
                .unwrap_or_else(empty_object)
        } else {
            input
        };

        let mut nodes: Vec<JsonValue> = Vec::new();
        let mut record_status = "ok".to_string();
        let mut record_error: Option<JsonValue> = None;
        let mut last_error_message: Option<String> = None;

        for (step_index, step) in endpoint.steps.iter().enumerate() {
            let step_input = current.clone();
            let step_started = Instant::now();
            if let Some(condition) = &step.when {
                let ctx = V2EvalContext::new();
                let keep = eval_v2_condition(
                    condition,
                    &current,
                    Some(&self.config_json()),
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
            let step_context = self.step_context(step.with.as_ref(), None);
            let step_result = self
                .execute_rule(&step.rule, &current, Some(&step_context), &self.endpoint_rule.base_dir)
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
                            .run_catch(catch, &err, &current, None, &self.endpoint_rule.base_dir)
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
                            .run_catch(catch, &err, &current, None, &self.endpoint_rule.base_dir)
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
                    record_error = Some(self.endpoint_error_to_trace(&err));
                    last_error_message = Some(err.message.clone());
                    let duration_us = step_started.elapsed().as_micros() as u64;
                    nodes.push(self.build_step_trace(
                        step_index,
                        step,
                        "error",
                        step_input,
                        None,
                        Some(err),
                        duration_us,
                        None,
                    ));
                    break;
                }
            }
        }

        let response_result = if record_status == "error" {
            Err(anyhow!(
                last_error_message.unwrap_or_else(|| "endpoint error".to_string())
            ))
        } else {
            self.build_reply(&endpoint.reply, &current)
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
        if let Err(err) = self.write_trace(&trace).await {
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
        let rule_path = rule_ref_from_path(&self.endpoint_rule.base_dir, &self.endpoint_rule.source_path);
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
            "timestamp": now.to_rfc3339(),
            "rule": {
                "type": "endpoint",
                "name": format!("{} {}", method.as_str(), path),
                "path": rule_path,
                "version": 2
            },
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
        json!({
            "code": format!("{:?}", err.kind),
            "message": err.message,
            "path": err.path.as_ref().map(|p| p.display().to_string())
        })
    }

    async fn write_trace(&self, trace: &JsonValue) -> Result<()> {
        let now = Utc::now();
        let trace_id = trace
            .get("trace_id")
            .and_then(|value| value.as_str())
            .unwrap_or("trace");
        let trace_dir = self
            .config
            .data_dir
            .join("traces")
            .join(format!("{:04}", now.year()))
            .join(format!("{:02}", now.month()))
            .join(format!("{:02}", now.day()));
        tokio::fs::create_dir_all(&trace_dir)
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        let path = trace_dir.join(format!("{}.json", trace_id));
        let payload = serde_json::to_string_pretty(trace)?;
        tokio::fs::write(&path, payload)
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        Ok(())
    }

    async fn execute_rule(
        &self,
        rule_path: &str,
        input: &JsonValue,
        context: Option<&JsonValue>,
        base_dir: &Path,
    ) -> Result<RuleExecution, EndpointError> {
        let resolved = resolve_rule_path(base_dir, rule_path);
        let rule_source = std::fs::read_to_string(&resolved)
            .ok()
            .and_then(|source| yaml_source_to_json(&source))
            .unwrap_or_else(|| json!({}));
        let rule_ref = rule_ref_from_path(base_dir, &resolved);
        match load_rule_kind(&resolved).map_err(|err| EndpointError::invalid(err.to_string()))? {
            RuleKind::Normal(rule) => {
                let output = transform_record_with_base_dir(
                    &rule.rule,
                    input,
                    context,
                    &rule.base_dir,
                )
                .map_err(EndpointError::from_transform)?
                .unwrap_or_else(empty_object);
                let nodes =
                    build_rule_nodes_from_rule(&rule.rule, input, context, &rule.base_dir);
                let duration_us = sum_node_duration_us(&nodes);
                let child_trace = build_rule_trace(
                    "normal",
                    rule_display_name(&resolved),
                    rule_ref,
                    rule.rule.version,
                    rule_source,
                    input.clone(),
                    output.clone(),
                    nodes,
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
                    .execute_network(&rule, input, context)
                    .await
                    .map_err(|err| err.with_path(resolved.clone()))?;
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
    ) -> Result<NetworkExecution, EndpointError> {
        if rule.request.method == Method::GET && rule.body.is_some() {
            return Err(EndpointError::invalid("GET with body is not allowed"));
        }

        let url = eval_expr_string(&rule.request.url, input, context)?;
        let headers = build_headers(&rule.request.headers)?;
        let body = self.build_network_body(rule, input, context)?;

        let total_started = Instant::now();
        let mut attempt = 0;
        loop {
            let request_started = Instant::now();
            let result = self
                .send_network_request(rule, &url, &headers, body.as_ref())
                .await;
            let request_us = request_started.elapsed().as_micros() as u64;

            match result {
                Ok(value) => {
                    if let Some(select) = &rule.select {
                        let tokens = parse_path(select).map_err(|_| {
                            EndpointError::invalid(format!("invalid select path: {}", select))
                        })?;
                        let selected = get_path(&value, &tokens).ok_or_else(|| {
                            EndpointError::invalid(format!("select path not found: {}", select))
                        })?;
                        return Ok(NetworkExecution {
                            output: selected.clone(),
                            request_us,
                            total_us: total_started.elapsed().as_micros() as u64,
                        });
                    }
                    return Ok(NetworkExecution {
                        output: value,
                        request_us,
                        total_us: total_started.elapsed().as_micros() as u64,
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
                    if let Some(catch) = &rule.catch {
                        if let Some(output) =
                            self.run_catch(catch, &err, input, None, &rule.base_dir)?
                        {
                            return Ok(NetworkExecution {
                                output,
                                request_us,
                                total_us: total_started.elapsed().as_micros() as u64,
                            });
                        }
                    }
                    return Err(err);
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
            let output =
                transform_record_with_base_dir(&body_rule.rule, input, context, &body_rule.base_dir)
                    .map_err(EndpointError::from_transform)?
                    .unwrap_or_else(empty_object);
            return Ok(Some(output));
        }
        Ok(None)
    }

    async fn send_network_request(
        &self,
        rule: &CompiledNetworkRule,
        url: &str,
        headers: &HeaderMap,
        body: Option<&JsonValue>,
    ) -> Result<JsonValue, EndpointError> {
        let mut req = self.client.request(rule.request.method.clone(), url);
        let mut headers = headers.clone();
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

        let response = tokio::time::timeout(rule.timeout, req.send())
            .await
            .map_err(|_| EndpointError::timeout())?
            .map_err(|err| EndpointError::network(err.to_string()))?;

        let status = response.status();
        let status_u16 = status.as_u16();
        if status.is_client_error() || status.is_server_error() {
            return Err(EndpointError::http_status(status_u16));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|err| EndpointError::network(err.to_string()))?;
        let value = if bytes.is_empty() {
            JsonValue::Null
        } else {
            serde_json::from_slice::<JsonValue>(&bytes)
                .map_err(|err| EndpointError::network(err.to_string()))?
        };
        Ok(value)
    }

    fn run_catch(
        &self,
        catch: &CatchSpec,
        error: &EndpointError,
        input: &JsonValue,
        params: Option<&JsonValue>,
        base_dir: &Path,
    ) -> Result<Option<JsonValue>, EndpointError> {
        if let Some(target) = catch.match_target(error) {
            let target_path = resolve_rule_path(base_dir, &target.to_string_lossy());
            let rule = match load_rule_kind(&target_path)
                .map_err(|err| EndpointError::invalid(err.to_string()))?
            {
                RuleKind::Normal(rule) => rule,
                RuleKind::Network(_) => {
                    return Err(EndpointError::invalid("catch rule must be normal"))
                }
            };
            let error_context = self.step_context(params, Some(error));
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

    fn build_reply(&self, reply: &CompiledReply, input: &JsonValue) -> Result<Response> {
        let status_value = eval_expr_value(&reply.status, input, Some(&self.config_json()))?;
        let status = match status_value {
            EvalValue::Value(JsonValue::Number(num)) => {
                num.as_u64().ok_or_else(|| anyhow!("status must be integer"))?
            }
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
            match eval_expr_value(body_expr, input, Some(&self.config_json()))? {
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
            let header_value = HeaderValue::from_str(value)
                .map_err(|_| anyhow!("invalid header value"))?;
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

    fn config_json(&self) -> JsonValue {
        json!({
            "config": {
                "internal_base": self.config.internal_base,
            }
        })
    }

    fn step_context(&self, params: Option<&JsonValue>, error: Option<&EndpointError>) -> JsonValue {
        let mut value = self.config_json();
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

#[derive(Debug)]
struct CompiledEndpointRule {
    base_dir: PathBuf,
    source_path: PathBuf,
    endpoints: Vec<CompiledEndpoint>,
}

impl CompiledEndpointRule {
    fn compile(raw: EndpointRuleFile, source_path: &Path) -> Result<Self> {
        let base_dir = source_path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
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
        let method = Method::from_bytes(raw.method.as_bytes())
            .map_err(|_| anyhow!("invalid method"))?;
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
        Ok(Self { status, headers, body })
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
    catch: Option<CatchSpec>,
    retry: Option<RetryConfig>,
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
    headers: Option<HashMap<String, String>>,
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
    headers: HashMap<String, String>,
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
) -> Result<JsonValue> {
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

    let query = parse_query(parts.uri.query())?;

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

    Ok(input)
}

fn build_headers(headers: &HashMap<String, String>) -> Result<HeaderMap, EndpointError> {
    let mut map = HeaderMap::new();
    for (key, value) in headers {
        let name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| EndpointError::invalid("invalid header name"))?;
        let header_value =
            HeaderValue::from_str(value).map_err(|_| EndpointError::invalid("invalid header value"))?;
        map.insert(name, header_value);
    }
    Ok(map)
}

fn parse_query(query: Option<&str>) -> Result<JsonValue> {
    let mut map: HashMap<String, String> = HashMap::new();
    if let Some(q) = query {
        for (key, value) in url::form_urlencoded::parse(q.as_bytes()) {
            let key = key.into_owned();
            let value = value.into_owned();
            if map.contains_key(&key) {
                return Err(anyhow!("duplicate query param: {}", key));
            }
            map.insert(key, value);
        }
    }
    Ok(serde_json::to_value(map)?)
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
    match eval_expr_value(expr, input, context).map_err(|err| {
        EndpointError::invalid(format!("expr eval error: {}", err))
    })? {
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
            push_error(
                errors,
                "ReadFailed",
                path,
                err.to_string(),
                None,
                None,
            );
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

fn parse_rule_type(
    path: &Path,
    source: &str,
    errors: &mut Vec<RulesDirError>,
) -> Option<String> {
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
            let base_dir = path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
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

    let method = Method::from_bytes(raw.request.method.as_bytes())
        .map_err(|_| anyhow!("invalid method"))?;
    if method == Method::GET && (raw.body.is_some() || raw.body_map.is_some() || raw.body_rule.is_some()) {
        return Err(anyhow!("GET with body is not allowed"));
    }
    let url_expr = parse_v2_expr(&raw.request.url).map_err(|err| anyhow!(err))?;
    let headers = raw
        .request
        .headers
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k.to_lowercase(), v))
        .collect();
    let timeout = parse_duration(&raw.timeout)?;
    if timeout.is_zero() {
        return Err(anyhow!("timeout must be > 0"));
    }
    let body = match raw.body {
        Some(value) => Some(parse_v2_expr(&value).map_err(|err| anyhow!(err))?),
        None => None,
    };
    let body_rule = match raw.body_rule {
        Some(path_str) => {
            let resolved =
                resolve_rule_path(path.parent().unwrap_or_else(|| Path::new(".")), &path_str);
            let source = std::fs::read_to_string(&resolved)
                .with_context(|| format!("failed to read {}", resolved.display()))?;
            let rule = parse_rule_file(&source)
                .with_context(|| format!("failed to parse {}", resolved.display()))?;
            validate_rule_file_with_source(&rule, &source)
                .map_err(|err| anyhow!("failed to validate {}: {:?}", resolved.display(), err))?;
            let base_dir = resolved.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
            Some(LoadedRule { rule, base_dir })
        }
        None => None,
    };

    let retry = compile_retry(raw.retry.as_ref())?;
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
        catch: raw.catch.map(CatchSpec::from),
        retry,
        base_dir: path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf(),
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
            RetryBackoff::Linear => self.initial_delay.checked_mul(factor).unwrap_or(Duration::MAX),
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

fn rule_ref_from_path(base_dir: &Path, path: &Path) -> String {
    if let Ok(rel) = path.strip_prefix(base_dir) {
        let rel = rel.to_string_lossy().replace('\\', "/");
        format!("rules/{}", rel)
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
    json!({
        "trace_id": trace_id,
        "timestamp": now.to_rfc3339(),
        "rule": {
            "type": rule_type,
            "name": name,
            "path": path,
            "version": version
        },
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

fn build_rule_nodes_from_rule(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Vec<JsonValue> {
    let mut nodes = Vec::new();
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
                    match eval_trace_condition(expr, record, context, &step_input, "record_when", rule.version) {
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
                        JsonValue::Array(
                            refs.iter().cloned().map(JsonValue::String).collect(),
                        ),
                    );
                    meta.insert(
                        "rule_ref_labels".to_string(),
                        JsonValue::Array(
                            labels.iter().cloned().map(JsonValue::String).collect(),
                        ),
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
                            let child_nodes = build_rule_nodes_from_rule(
                                &loaded.rule,
                                &step_input,
                                context,
                                &loaded.base_dir,
                            );
                            let child_duration_us = sum_node_duration_us(&child_nodes);
                            let child_output = transform_record_with_base_dir(
                                &loaded.rule,
                                &step_input,
                                context,
                                &loaded.base_dir,
                            )
                            .ok()
                            .and_then(|value| value)
                            .unwrap_or_else(empty_object);
                            child_trace = Some(build_rule_trace(
                                "normal",
                                rule_display_name(&resolved),
                                rule_ref_from_path(base_dir, &resolved),
                                loaded.rule.version,
                                rule_source,
                                step_input.clone(),
                                child_output,
                                child_nodes,
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

        let mut node = json!({
            "id": "step-finalize",
            "kind": "finalize",
            "label": "finalize",
            "status": finalize_status,
            "input": finalize_input,
            "output": finalize_output,
            "duration_us": finalize_duration_us,
        });
        if let Some(err) = finalize_error {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("error".to_string(), err);
            }
        }
        if !children.is_empty() {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("children".to_string(), JsonValue::Array(children));
            }
        }
        nodes.push(node);
    }

    nodes
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

    Err(
        TransformError::new(
            TransformErrorKind::ExprError,
            "when/record_when must evaluate to boolean",
        )
        .with_path(path),
    )
}

fn build_network_nodes_with_timing(
    rule: &CompiledNetworkRule,
    timing: &NetworkExecution,
) -> Vec<JsonValue> {
    let mut children = Vec::new();
    let mut request_args = JsonMap::new();
    request_args.insert("method".to_string(), JsonValue::String(rule.request.method.to_string()));
    request_args.insert("url".to_string(), JsonValue::String(format!("{:?}", rule.request.url)));
    if !rule.request.headers.is_empty() {
        request_args.insert(
            "headers".to_string(),
            serde_json::to_value(&rule.request.headers).unwrap_or_else(|_| json!({})),
        );
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
        args.insert("target".to_string(), JsonValue::String(mapping.target.clone()));
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
            let args: Vec<JsonValue> = op.args.iter().map(expr_to_json_value_for_condition).collect();
            let mut obj = JsonMap::new();
            obj.insert(op.op.clone(), JsonValue::Array(args));
            JsonValue::Object(obj)
        }
        Expr::Chain(chain) => {
            let items: Vec<JsonValue> = chain.chain.iter().map(expr_to_json_value_for_condition).collect();
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
    let start_output = start_value
        .clone()
        .and_then(eval_value_to_json);
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
                if let Ok(next) =
                    eval_v2_op_step(op_step, current.clone(), record, context, out, &step_path, &current_ctx)
                {
                    current = next;
                }
            }
            V2Step::Let(let_step) => {
                if let Ok(next_ctx) =
                    eval_v2_let_step(let_step, current.clone(), record, context, out, &step_path, &current_ctx)
                {
                    current_ctx = next_ctx;
                }
            }
            V2Step::If(if_step) => {
                if let Ok(next) =
                    eval_v2_if_step(if_step, current.clone(), record, context, out, &step_path, &current_ctx)
                {
                    current = next;
                }
            }
            V2Step::Map(map_step) => {
                if let Ok(next) =
                    eval_v2_map_step(map_step, current.clone(), record, context, out, &step_path, &current_ctx)
                {
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
            JsonValue::Object(map) => map.entry(key.to_string()).or_insert_with(|| {
                JsonValue::Object(JsonMap::new())
            }),
            _ => {
                *current = JsonValue::Object(JsonMap::new());
                if let JsonValue::Object(map) = current {
                    map.entry(key.to_string()).or_insert_with(|| {
                        JsonValue::Object(JsonMap::new())
                    })
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
    use serde_json::json;

    #[test]
    fn endpoint_path_matches_and_captures() {
        let path = EndpointPath::parse("/api/traces/{id}").unwrap();
        assert!(path.matches("/api/traces/abc"));
        let params = path.capture("/api/traces/abc");
        assert_eq!(params.get("id"), Some(&"abc".to_string()));
    }

    #[test]
    fn compile_retry_defaults_to_none() {
        let retry = compile_retry(None).unwrap();
        assert!(retry.is_none());
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
            select: None,
            body: None,
            body_map: None,
            body_rule: None,
            catch: None,
            retry: None,
        };
        let err = compile_network_rule(raw, Path::new("network.yaml"))
            .expect_err("expected error");
        assert!(err.to_string().contains("timeout must be > 0"));
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
            EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf()),
        )
        .expect("load engine");

        let request = Request::builder()
            .method("GET")
            .uri("/api/empty")
            .body(axum::body::Body::empty())
            .expect("build request");

        let response = engine.handle_request(request).await.expect("handle request");
        assert_eq!(response.status().as_u16(), 204);
        assert!(response.headers().get("content-type").is_none());

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert!(bytes.is_empty());
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
        let nodes = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
        let duration = nodes[0].get("duration_us").and_then(|value| value.as_u64());
        assert!(duration.is_some());
    }

    #[test]
    fn network_nodes_include_request_duration_us() {
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
            body_rule: None,
            catch: None,
            retry: None,
            base_dir: PathBuf::from("."),
        };
        let timing = NetworkExecution {
            output: json!({}),
            request_us: 12,
            total_us: 34,
        };

        let nodes = build_network_nodes_with_timing(&rule, &timing);
        let duration = nodes[0].get("duration_us").and_then(|value| value.as_u64());
        assert_eq!(duration, Some(34));

        let children = nodes[0]
            .get("children")
            .and_then(|value| value.as_array())
            .expect("children");
        assert_eq!(children.len(), 1);
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
