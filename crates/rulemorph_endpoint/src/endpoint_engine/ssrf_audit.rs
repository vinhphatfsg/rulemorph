use axum::http::Method;

use super::config::RequestContext;
use super::network_rule::CompiledNetworkRule;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SsrAuditLog {
    pub(super) tenant_id: String,
    pub(super) rule_ref: String,
    pub(super) method: Method,
    pub(super) url: String,
    pub(super) reason: String,
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

pub(super) fn build_ssrf_audit_log(
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
