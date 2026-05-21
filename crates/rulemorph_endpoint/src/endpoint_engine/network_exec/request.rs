use anyhow::Result;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use serde_json::Value as JsonValue;

use crate::ssrf::{ResolvedSsrfTarget, resolve_ssrf_target};

use super::response;
use crate::endpoint_engine::EndpointEngine;
use crate::endpoint_engine::config::RequestContext;
use crate::endpoint_engine::error::EndpointError;
use crate::endpoint_engine::network_rule::CompiledNetworkRule;
use crate::endpoint_engine::ssrf_audit::build_ssrf_audit_log;

impl EndpointEngine {
    fn build_resolved_client(&self, target: &ResolvedSsrfTarget) -> Result<Client, EndpointError> {
        Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .resolve(&target.host, target.addr)
            .build()
            .map_err(|err| EndpointError::network(err.to_string()))
    }

    pub(in crate::endpoint_engine) async fn send_network_request(
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
                self.apply_internal_auth_headers(&mut headers, request_context)?;
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
