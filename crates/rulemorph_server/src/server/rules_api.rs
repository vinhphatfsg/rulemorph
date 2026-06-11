use std::sync::Arc;

use axum::{extract::State, http::Request, middleware::Next};
use rulemorph_endpoint::{EndpointEngine, RequestContext};

use crate::TenantContext;

use super::{
    ApiError, AppState, TenantResources,
    auth::{
        DispatchInvalidApiKeyForRules, apply_v1_auth_context, enforce_api_rate_limit,
        ensure_pre_auth_rate_limit_for_request, rate_limit_key,
    },
};

pub(super) fn request_resources(
    state: &AppState,
    request: &Request<axum::body::Body>,
) -> Arc<TenantResources> {
    request
        .extensions()
        .get::<Arc<TenantResources>>()
        .cloned()
        .unwrap_or_else(|| state.default_resources.clone())
}

pub(super) fn request_engine<'a>(
    state: &'a AppState,
    resources: &'a Arc<TenantResources>,
) -> std::result::Result<&'a Arc<EndpointEngine>, ApiError> {
    resources
        .api_engine
        .as_ref()
        .or(state.default_resources.api_engine.as_ref())
        .ok_or_else(|| ApiError::internal("api engine not configured"))
}

pub(super) async fn run_rules_api_request(
    state: &AppState,
    mut request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    if state.tenant_resolver.is_some() && state.rate_limiter.is_some() {
        ensure_pre_auth_rate_limit_for_request(state, &mut request).await?;
    }
    if state.tenant_resolver.is_some() && request.extensions().get::<TenantContext>().is_none() {
        if request
            .extensions()
            .get::<DispatchInvalidApiKeyForRules>()
            .is_some()
        {
            return Err(ApiError::unauthorized("invalid api key"));
        }
        apply_v1_auth_context(state, &mut request).await?;
    }
    if state.rate_limiter.is_some() {
        let key = rate_limit_key(state, &request);
        enforce_api_rate_limit(state, key).await?;
    }
    handle_rules_api_core(state, request).await
}

pub(super) async fn handle_rules_api(
    state: State<AppState>,
    request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    handle_rules_api_core(&state.0, request).await
}

async fn handle_rules_api_core(
    state: &AppState,
    mut request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    let resources = request_resources(state, &request);
    let engine = request_engine(state, &resources)?;
    let mut request_context = RequestContext::default();
    if let Some(context) = request.extensions().get::<TenantContext>() {
        request_context.tenant_id = Some(context.tenant_id.clone());
    }
    if engine.allows_internal_auth()
        && let Some(internal_api_key) = state.internal_api_key.clone()
    {
        request_context.internal_api_key = Some(internal_api_key);
    }
    request.extensions_mut().insert(request_context);
    match engine.handle_request(request).await {
        Ok(response) => Ok(response),
        Err(err) => {
            let message = err.to_string();
            if message.contains("no endpoint matched") {
                Err(ApiError::not_found(message))
            } else {
                Err(ApiError::internal(message))
            }
        }
    }
}

pub(super) async fn inject_default_resources(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    if request.extensions().get::<Arc<TenantResources>>().is_none() {
        request
            .extensions_mut()
            .insert(state.default_resources.clone());
    }
    Ok(next.run(request).await)
}
