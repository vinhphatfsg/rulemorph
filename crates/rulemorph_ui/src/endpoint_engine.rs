use std::collections::HashMap;
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
    get_path, parse_path, parse_rule_file, transform_record, validate_rule_file_with_source, Expr,
    Mapping, RuleFile, TransformError,
};
use serde::Deserialize;
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
                    nodes.push(self.build_step_trace(
                        step_index,
                        step,
                        "skipped",
                        step_input,
                        Some(current.clone()),
                        None,
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
                    nodes.push(self.build_step_trace(
                        step_index,
                        step,
                        "ok",
                        step_input,
                        Some(execution.output),
                        None,
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
                            nodes.push(self.build_step_trace(
                                step_index,
                                step,
                                "ok",
                                step_input,
                                Some(next),
                                None,
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
                            nodes.push(self.build_step_trace(
                                step_index,
                                step,
                                "ok",
                                step_input,
                                Some(next),
                                None,
                                None,
                            ));
                            break;
                        }
                    }

                    record_status = "error".to_string();
                    record_error = Some(self.endpoint_error_to_trace(&err));
                    last_error_message = Some(err.message.clone());
                    nodes.push(self.build_step_trace(
                        step_index,
                        step,
                        "error",
                        step_input,
                        None,
                        Some(err),
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

        let duration_ms = started.elapsed().as_millis() as u64;
        let trace = self.build_trace(
            &method,
            &path,
            record_input,
            current.clone(),
            record_status,
            record_error,
            nodes,
            duration_ms,
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
        duration_ms: u64,
    ) -> JsonValue {
        let trace_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let rule_path = rule_ref_from_path(&self.endpoint_rule.base_dir, &self.endpoint_rule.source_path);
        let rule_source = self.raw_rule_source.clone();
        let record = json!({
            "index": 0,
            "status": status,
            "duration_ms": duration_ms,
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
                "duration_ms": duration_ms
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
                let output = transform_record(&rule, input, context)
                    .map_err(EndpointError::from_transform)?
                    .unwrap_or_else(empty_object);
                let nodes = build_rule_nodes_from_rule(&rule, input, context);
                let child_trace = build_rule_trace(
                    "normal",
                    rule_display_name(&resolved),
                    rule_ref,
                    rule.version,
                    rule_source,
                    input.clone(),
                    output.clone(),
                    nodes,
                    "ok",
                );
                Ok(RuleExecution {
                    output,
                    child_trace: Some(child_trace),
                })
            }
            RuleKind::Network(rule) => {
                let output = self
                    .execute_network(&rule, input, context)
                    .await
                    .map_err(|err| err.with_path(resolved.clone()))?;
                let nodes = build_network_nodes(&rule);
                let child_trace = build_rule_trace(
                    "network",
                    rule_display_name(&resolved),
                    rule_ref,
                    2,
                    rule_source,
                    input.clone(),
                    output.clone(),
                    nodes,
                    "ok",
                );
                Ok(RuleExecution {
                    output,
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
    ) -> Result<JsonValue, EndpointError> {
        if rule.request.method == Method::GET && rule.body.is_some() {
            return Err(EndpointError::invalid("GET with body is not allowed"));
        }

        let url = eval_expr_string(&rule.request.url, input, context)?;
        let headers = build_headers(&rule.request.headers)?;
        let body = self.build_network_body(rule, input, context)?;

        let mut attempt = 0;
        loop {
            let result = self
                .send_network_request(rule, &url, &headers, body.as_ref())
                .await;

            match result {
                Ok(value) => {
                    if let Some(select) = &rule.select {
                        let tokens = parse_path(select).map_err(|_| {
                            EndpointError::invalid(format!("invalid select path: {}", select))
                        })?;
                        let selected = get_path(&value, &tokens).ok_or_else(|| {
                            EndpointError::invalid(format!("select path not found: {}", select))
                        })?;
                        return Ok(selected.clone());
                    }
                    return Ok(value);
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
                            return Ok(output);
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
            let output = transform_record(body_rule, input, context)
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
            let output = transform_record(&rule, input, Some(&error_context))
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
                EvalValue::Missing => JsonValue::Null,
                EvalValue::Value(value) => value,
            }
        } else {
            JsonValue::Null
        };

        let mut headers = HeaderMap::new();
        for (key, value) in &reply.headers {
            let name = HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| anyhow!("invalid header name"))?;
            let header_value = HeaderValue::from_str(value)
                .map_err(|_| anyhow!("invalid header value"))?;
            headers.insert(name, header_value);
        }
        if !headers.contains_key("content-type") {
            headers.insert(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            );
        }

        let mut response = Response::new(axum::body::Body::from(
            serde_json::to_vec(&body).unwrap_or_else(|_| b"null".to_vec()),
        ));
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
    body_rule: Option<RuleFile>,
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
            Ok(RuleKind::Normal(rule))
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
            let resolved = resolve_rule_path(path.parent().unwrap_or_else(|| Path::new(".")), &path_str);
            let source = std::fs::read_to_string(&resolved)
                .with_context(|| format!("failed to read {}", resolved.display()))?;
            let rule = parse_rule_file(&source)
                .with_context(|| format!("failed to parse {}", resolved.display()))?;
            validate_rule_file_with_source(&rule, &source)
                .map_err(|err| anyhow!("failed to validate {}: {:?}", resolved.display(), err))?;
            Some(rule)
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
    status: &str,
) -> JsonValue {
    let trace_id = Uuid::new_v4().to_string();
    let now = Utc::now();
    let record = json!({
        "index": 0,
        "status": status,
        "duration_ms": 0,
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
            "duration_ms": 0
        }
    })
}

fn build_rule_nodes_from_rule(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
) -> Vec<JsonValue> {
    let mut nodes = Vec::new();
    let mut out = JsonValue::Object(JsonMap::new());
    if let Some(steps) = &rule.steps {
        for (index, step) in steps.iter().enumerate() {
            let label = step
                .name
                .clone()
                .unwrap_or_else(|| format!("step-{}", index + 1));
            let children = build_mapping_ops_with_values(
                step.mappings.as_deref().unwrap_or(&[]),
                record,
                context,
                &mut out,
                rule.version,
                index,
            );
            let mut node = json!({
                "id": format!("step-{}", index),
                "kind": "step",
                "label": label,
                "status": "ok",
            });
            if !children.is_empty() {
                if let Some(obj) = node.as_object_mut() {
                    obj.insert("children".to_string(), JsonValue::Array(children));
                }
            }
            nodes.push(node);
        }
    } else {
        let children = build_mapping_ops_with_values(
            &rule.mappings,
            record,
            context,
            &mut out,
            rule.version,
            0,
        );
        let mut node = json!({
            "id": "step-0",
            "kind": "mapping",
            "label": "mappings",
            "status": "ok",
        });
        if !children.is_empty() {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("children".to_string(), JsonValue::Array(children));
            }
        }
        nodes.push(node);
    }

    if let Some(finalize) = &rule.finalize {
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
            "status": "ok",
        });
        if !children.is_empty() {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("children".to_string(), JsonValue::Array(children));
            }
        }
        nodes.push(node);
    }

    nodes
}

fn build_network_nodes(rule: &CompiledNetworkRule) -> Vec<JsonValue> {
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
    Normal(RuleFile),
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
}
