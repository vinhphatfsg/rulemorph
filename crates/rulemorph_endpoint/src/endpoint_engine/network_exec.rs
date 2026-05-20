use std::time::Instant;

use anyhow::Result;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use rulemorph::{get_path, parse_path};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::ssrf::{ResolvedSsrfTarget, resolve_ssrf_target};

mod body;
mod response;

use super::config::RequestContext;
use super::error::{EndpointError, EndpointErrorKind};
use super::expr::{build_headers, eval_expr_string};
use super::host::internal_hosts_match;
use super::network_rule::CompiledNetworkRule;
use super::ssrf_audit::build_ssrf_audit_log;
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

    fn build_resolved_client(&self, target: &ResolvedSsrfTarget) -> Result<Client, EndpointError> {
        Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .resolve(&target.host, target.addr)
            .build()
            .map_err(|err| EndpointError::network(err.to_string()))
    }

    pub(super) fn is_internal_target(&self, url: &str) -> bool {
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

    pub(super) fn resolve_internal_api_key(
        &self,
        request_context: Option<&RequestContext>,
    ) -> Option<String> {
        if let Some(context) = request_context {
            if let Some(key) = context.internal_api_key.as_ref() {
                return Some(key.clone());
            }
        }
        self.config.internal_api_key.clone()
    }

    pub(super) fn context_with_internal_api_key(
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

    pub(super) async fn send_network_request(
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

            let response = req
                .send()
                .await
                .map_err(|err| EndpointError::network(err.to_string()))?;

            response::read_network_response(response, self.config.max_response_bytes).await
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
}
