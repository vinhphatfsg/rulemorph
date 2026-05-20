use axum::{
    extract::State,
    http::{HeaderMap, Request},
    middleware::Next,
};

use crate::{TenantContext, validate_tenant_id};

use super::{ApiError, AppState};

mod api_key;
mod rate_limit;

use self::api_key::extract_api_key;
#[cfg(test)]
pub(super) use self::rate_limit::pre_auth_rate_limit_key;
pub(super) use self::rate_limit::{
    DispatchInvalidApiKeyForRules, InternalApiRateLimitScope, api_rate_limit,
    enforce_api_rate_limit, ensure_pre_auth_rate_limit_for_request, pre_auth_rate_limit,
    rate_limit_key,
};

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
