use axum::{
    extract::{ConnectInfo, State},
    http::Request,
    middleware::Next,
};
use sha2::{Digest, Sha256};

use crate::TenantContext;

use super::api_key::extract_api_key;
use super::{ApiError, AppState};

#[derive(Clone, Copy, Debug)]
struct PreAuthRateLimitApplied;

#[derive(Clone, Copy, Debug)]
pub(in crate::server) struct InternalApiRateLimitScope;

#[derive(Clone, Copy, Debug)]
pub(in crate::server) struct DispatchInvalidApiKeyForRules;

pub(in crate::server) async fn ensure_pre_auth_rate_limit_for_request(
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
    if let Some(limiter) = state.rate_limiter.as_ref()
        && !limiter.allow(&key).await
    {
        return Err(ApiError::too_many_requests("rate limit exceeded"));
    }
    Ok(())
}

pub(in crate::server) async fn pre_auth_rate_limit(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    ensure_pre_auth_rate_limit_for_request(&state, &mut request).await?;
    Ok(next.run(request).await)
}

#[cfg(test)]
pub(in crate::server) fn pre_auth_rate_limit_key(request: &Request<axum::body::Body>) -> String {
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

pub(in crate::server) async fn enforce_api_rate_limit(
    state: &AppState,
    key: String,
) -> std::result::Result<(), ApiError> {
    if let Some(limiter) = state.rate_limiter.as_ref()
        && !limiter.allow(&key).await
    {
        return Err(ApiError::too_many_requests("rate limit exceeded"));
    }
    Ok(())
}

pub(in crate::server) async fn api_rate_limit(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    let key = rate_limit_key(&state, &request);
    enforce_api_rate_limit(&state, key).await?;
    Ok(next.run(request).await)
}

pub(in crate::server) fn rate_limit_key(
    state: &AppState,
    request: &Request<axum::body::Body>,
) -> String {
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
