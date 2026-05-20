use anyhow::Result;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::*;
use crate::endpoint_engine::config::RequestContext;
use crate::endpoint_engine::error::EndpointError;
use crate::endpoint_engine::host::internal_hosts_match;

impl EndpointEngine {
    pub(in crate::endpoint_engine) fn is_internal_target(&self, url: &str) -> bool {
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

    pub(in crate::endpoint_engine) fn resolve_internal_api_key(
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

    pub(in crate::endpoint_engine) fn context_with_internal_api_key(
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

    pub(super) fn ensure_internal_auth_path_allowed(&self, url: &str) -> Result<(), EndpointError> {
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

    pub(super) fn apply_internal_auth_headers(
        &self,
        headers: &mut HeaderMap,
        request_context: Option<&RequestContext>,
    ) -> Result<(), EndpointError> {
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
        Ok(())
    }
}
