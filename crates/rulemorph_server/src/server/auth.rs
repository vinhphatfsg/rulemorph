use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, Request},
    middleware::Next,
};
use sha2::{Digest, Sha256};

use crate::{TenantContext, validate_tenant_id};

use super::{ApiError, AppState};

pub(super) async fn apply_v1_auth_context(
    state: &AppState,
    request: &mut Request<axum::body::Body>,
) -> std::result::Result<(), ApiError> {
    if state.tenant_resolver.is_none() {
        return Err(ApiError::service_unavailable(
            "tenant resolver not configured",
        ));
    }
    if request.extensions().get::<TenantContext>().is_some() {
        return Ok(());
    }
    let api_key = extract_api_key(request.headers())
        .ok_or_else(|| ApiError::unauthorized("missing api key"))?;
    let context = resolve_v1_tenant_context(state, &api_key).await?;
    let Some(context) = context else {
        return Err(ApiError::unauthorized("invalid api key"));
    };
    attach_tenant_context(state, request, context).await?;
    Ok(())
}

async fn resolve_v1_tenant_context(
    state: &AppState,
    api_key: &str,
) -> std::result::Result<Option<TenantContext>, ApiError> {
    let resolver = state
        .tenant_resolver
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("tenant resolver not configured"))?;
    let context = resolver
        .resolve(api_key)
        .await
        .map_err(ApiError::internal)?;
    if let Some(context) = context.as_ref() {
        validate_tenant_id(&context.tenant_id)
            .map_err(|err| ApiError::bad_request(format!("invalid tenant_id: {}", err)))?;
    }
    Ok(context)
}

async fn attach_tenant_context(
    state: &AppState,
    request: &mut Request<axum::body::Body>,
    context: TenantContext,
) -> std::result::Result<(), ApiError> {
    if let Some(registry) = state.tenant_registry.as_ref() {
        let resources = registry
            .get_or_init(&context.tenant_id)
            .await
            .map_err(ApiError::internal)?;
        request.extensions_mut().insert(resources);
    }
    request.extensions_mut().insert(context);
    Ok(())
}

pub(super) async fn maybe_apply_v1_auth_context_for_dispatch(
    state: &AppState,
    request: &mut Request<axum::body::Body>,
) -> std::result::Result<(), ApiError> {
    if state.tenant_resolver.is_none() || request.extensions().get::<TenantContext>().is_some() {
        return Ok(());
    }
    let Some(api_key) = extract_api_key(request.headers()) else {
        return Ok(());
    };
    let Some(context) = resolve_v1_tenant_context(state, &api_key).await? else {
        request
            .extensions_mut()
            .insert(DispatchInvalidApiKeyForRules);
        return Ok(());
    };
    attach_tenant_context(state, request, context).await
}

pub(super) async fn v1_auth(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    apply_v1_auth_context(&state, &mut request).await?;
    Ok(next.run(request).await)
}

pub(super) async fn apply_internal_tenant_context(
    state: &AppState,
    request: &mut Request<axum::body::Body>,
) -> std::result::Result<(), ApiError> {
    if state.tenant_resolver.is_none() {
        return Ok(());
    }
    let tenant_id = request
        .headers()
        .get("x-tenant-id")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("missing x-tenant-id"))?;
    validate_tenant_id(&tenant_id)
        .map_err(|err| ApiError::bad_request(format!("invalid tenant_id: {}", err)))?;
    if let Some(registry) = state.tenant_registry.as_ref() {
        let resources = registry
            .get_or_init(&tenant_id)
            .await
            .map_err(ApiError::internal)?;
        request.extensions_mut().insert(resources);
    }
    request
        .extensions_mut()
        .insert(TenantContext::new(tenant_id));
    Ok(())
}

pub(super) async fn internal_tenant(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    apply_internal_tenant_context(&state, &mut request).await?;
    Ok(next.run(request).await)
}

pub(super) fn ensure_internal_auth(
    state: &AppState,
    headers: &HeaderMap,
) -> std::result::Result<(), ApiError> {
    let Some(expected) = state.internal_api_key.as_deref() else {
        if state.allow_unauth_internal && state.tenant_resolver.is_none() {
            return Ok(());
        }
        return Err(ApiError::service_unavailable(
            "internal api key not configured",
        ));
    };
    let provided = extract_api_key(headers)
        .ok_or_else(|| ApiError::unauthorized("missing internal api key"))?;
    if provided != expected {
        return Err(ApiError::unauthorized("invalid internal api key"));
    }
    Ok(())
}

pub(super) fn ensure_internal_auth_required(
    state: &AppState,
    headers: &HeaderMap,
) -> std::result::Result<(), ApiError> {
    let Some(expected) = state.internal_api_key.as_deref() else {
        return Err(ApiError::service_unavailable(
            "internal api key not configured",
        ));
    };
    let provided = extract_api_key(headers)
        .ok_or_else(|| ApiError::unauthorized("missing internal api key"))?;
    if provided != expected {
        return Err(ApiError::unauthorized("invalid internal api key"));
    }
    Ok(())
}

pub(super) async fn internal_auth(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    ensure_internal_auth(&state, request.headers())?;
    Ok(next.run(request).await)
}

pub(super) async fn internal_auth_required(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    ensure_internal_auth_required(&state, request.headers())?;
    Ok(next.run(request).await)
}

#[derive(Clone, Copy, Debug)]
struct PreAuthRateLimitApplied;

#[derive(Clone, Copy, Debug)]
pub(super) struct InternalApiRateLimitScope;

#[derive(Clone, Copy, Debug)]
pub(super) struct DispatchInvalidApiKeyForRules;

pub(super) async fn ensure_pre_auth_rate_limit_for_request(
    state: &AppState,
    request: &mut Request<axum::body::Body>,
) -> std::result::Result<(), ApiError> {
    if state.rate_limiter.is_none()
        || request
            .extensions()
            .get::<PreAuthRateLimitApplied>()
            .is_some()
    {
        return Ok(());
    }
    let key = pre_auth_rate_limit_key(request);
    enforce_pre_auth_rate_limit(state, key).await?;
    request.extensions_mut().insert(PreAuthRateLimitApplied);
    Ok(())
}

async fn enforce_pre_auth_rate_limit(
    state: &AppState,
    key: String,
) -> std::result::Result<(), ApiError> {
    if let Some(limiter) = state.rate_limiter.as_ref() {
        if !limiter.allow(&key).await {
            return Err(ApiError::too_many_requests("rate limit exceeded"));
        }
    }
    Ok(())
}

pub(super) async fn pre_auth_rate_limit(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    ensure_pre_auth_rate_limit_for_request(&state, &mut request).await?;
    Ok(next.run(request).await)
}

#[cfg(test)]
pub(super) fn pre_auth_rate_limit_key(request: &Request<axum::body::Body>) -> String {
    pre_auth_rate_limit_key_impl(request)
}

#[cfg(not(test))]
fn pre_auth_rate_limit_key(request: &Request<axum::body::Body>) -> String {
    pre_auth_rate_limit_key_impl(request)
}

fn pre_auth_rate_limit_key_impl(request: &Request<axum::body::Body>) -> String {
    if let Some(api_key) = extract_api_key(request.headers())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return format!("preauth:key:{}", short_hash(&api_key));
    }
    if let Some(ConnectInfo(addr)) = request
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
    {
        return format!("preauth:ip:{}", addr.ip());
    }
    "preauth:anonymous".to_string()
}

fn short_hash(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(super) async fn enforce_api_rate_limit(
    state: &AppState,
    key: String,
) -> std::result::Result<(), ApiError> {
    if let Some(limiter) = state.rate_limiter.as_ref() {
        if !limiter.allow(&key).await {
            return Err(ApiError::too_many_requests("rate limit exceeded"));
        }
    }
    Ok(())
}

pub(super) async fn api_rate_limit(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    let key = rate_limit_key(&state, &request);
    enforce_api_rate_limit(&state, key).await?;
    Ok(next.run(request).await)
}

pub(super) fn rate_limit_key(state: &AppState, request: &Request<axum::body::Body>) -> String {
    if let Some(context) = request.extensions().get::<TenantContext>() {
        return format!("tenant:{}", context.tenant_id);
    }
    if request
        .extensions()
        .get::<InternalApiRateLimitScope>()
        .is_some()
    {
        return "internal".to_string();
    }
    if state.internal_api_key.is_some() && request.uri().path().starts_with("/internal") {
        return "internal".to_string();
    }
    "global".to_string()
}

fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(axum::http::header::AUTHORIZATION) {
        let value = value.to_str().ok()?;
        let mut parts = value.split_whitespace();
        let scheme = parts.next()?;
        if scheme.eq_ignore_ascii_case("bearer") {
            return parts.next().map(|part| part.to_string());
        }
    }
    headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
}
