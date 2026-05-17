use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Json,
    extract::{Extension, FromRequest, Multipart, State},
    http::{HeaderMap, Method, Request},
    response::IntoResponse,
};
use rulemorph_trace::ImportResult;
use serde::Deserialize;

use super::auth::{
    InternalApiRateLimitScope, apply_internal_tenant_context, enforce_api_rate_limit,
    ensure_internal_auth, ensure_internal_auth_required, ensure_pre_auth_rate_limit_for_request,
    maybe_apply_v1_auth_context_for_dispatch, rate_limit_key,
};
use super::import_zip::{extract_zip, resolve_bundle_root, validate_bundle_path};
use super::rules_api::{request_engine, request_resources, run_rules_api_request};
use super::{ApiError, AppState, IMPORT_ZIP_MAX_TOTAL_BYTES, TenantResources};

fn is_multipart_form_data(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|part| part.trim().eq_ignore_ascii_case("multipart/form-data"))
        })
        .unwrap_or(false)
}

fn is_zip_import_request(request: &Request<axum::body::Body>) -> bool {
    request.method() == Method::POST
        && request.uri().path() == "/api/import"
        && is_multipart_form_data(request.headers())
}

pub(super) async fn handle_api_import_only(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    if !is_zip_import_request(&request) {
        return Err(ApiError::not_found("no endpoint matched"));
    }
    run_api_import_request(&state, request, false).await
}

pub(super) async fn handle_api_import_or_rules(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    handle_api_import_or_rules_with_auth_mode(state, request, false).await
}

pub(super) async fn handle_api_import_or_rules_strict(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    handle_api_import_or_rules_with_auth_mode(state, request, true).await
}

async fn handle_api_import_or_rules_with_auth_mode(
    state: AppState,
    mut request: Request<axum::body::Body>,
    require_internal_key: bool,
) -> std::result::Result<axum::response::Response, ApiError> {
    if !is_zip_import_request(&request) {
        return run_rules_api_request(&state, request).await;
    }
    if state.tenant_resolver.is_some() && state.rate_limiter.is_some() {
        ensure_pre_auth_rate_limit_for_request(&state, &mut request).await?;
    }
    if state.tenant_resolver.is_none() || request.headers().contains_key("x-tenant-id") {
        if require_internal_key {
            ensure_internal_auth_required(&state, request.headers())?;
        } else {
            ensure_internal_auth(&state, request.headers())?;
        }
        if state.tenant_resolver.is_some() {
            apply_internal_tenant_context(&state, &mut request).await?;
        }
    }
    maybe_apply_v1_auth_context_for_dispatch(&state, &mut request).await?;
    let resources = request_resources(&state, &request);
    let has_rule = request_engine(&state, &resources)
        .map(|engine| engine.has_endpoint(request.method(), request.uri().path()))
        .unwrap_or(false);
    if has_rule {
        return run_rules_api_request(&state, request).await;
    }
    run_api_import_request(&state, request, require_internal_key).await
}

async fn run_api_import_request(
    state: &AppState,
    mut request: Request<axum::body::Body>,
    require_internal_key: bool,
) -> std::result::Result<axum::response::Response, ApiError> {
    if state.rate_limiter.is_some() {
        ensure_pre_auth_rate_limit_for_request(state, &mut request).await?;
    }
    if require_internal_key {
        ensure_internal_auth_required(state, request.headers())?;
    } else {
        ensure_internal_auth(state, request.headers())?;
    }
    if state.tenant_resolver.is_some() {
        apply_internal_tenant_context(state, &mut request).await?;
    }
    if state.rate_limiter.is_some() {
        request.extensions_mut().insert(InternalApiRateLimitScope);
        let key = rate_limit_key(state, &request);
        enforce_api_rate_limit(state, key).await?;
    }
    import_bundle_zip_from_request(state, request).await
}

#[derive(Deserialize)]
pub(super) struct ImportPathRequest {
    bundle_path: String,
}

pub(super) async fn import_bundle_path(
    Extension(resources): Extension<Arc<TenantResources>>,
    Json(payload): Json<ImportPathRequest>,
) -> std::result::Result<Json<ImportResult>, ApiError> {
    let bundle_path = validate_bundle_path(&PathBuf::from(payload.bundle_path))?;
    let result = resources
        .store
        .import_bundle(&bundle_path)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(result))
}

async fn import_bundle_zip_from_request(
    state: &AppState,
    request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    let resources = request_resources(state, &request);
    let multipart = Multipart::from_request(request, state)
        .await
        .map_err(|err| ApiError::bad_request(format!("multipart error: {}", err)))?;
    let result = import_bundle_zip_with_resources(resources, multipart).await?;
    Ok(Json(result).into_response())
}

async fn import_bundle_zip_with_resources(
    resources: Arc<TenantResources>,
    mut multipart: Multipart,
) -> std::result::Result<ImportResult, ApiError> {
    let mut zip_file: Option<tempfile::NamedTempFile> = None;
    let mut total_bytes: u64 = 0;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|err| ApiError::bad_request(format!("multipart error: {}", err)))?
    {
        let name = field.name().map(|value| value.to_string());
        if name.as_deref() != Some("bundle") {
            continue;
        }
        let mut handle =
            tempfile::NamedTempFile::new().map_err(|err| ApiError::internal(err.to_string()))?;
        let mut field = field;
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|err| ApiError::bad_request(format!("upload error: {}", err)))?
        {
            total_bytes = total_bytes.saturating_add(chunk.len() as u64);
            if total_bytes > IMPORT_ZIP_MAX_TOTAL_BYTES {
                return Err(ApiError::bad_request("zip exceeds max size"));
            }
            handle
                .write_all(&chunk)
                .map_err(|err| ApiError::internal(err.to_string()))?;
        }
        zip_file = Some(handle);
        break;
    }
    let zip_file = zip_file.ok_or_else(|| ApiError::bad_request("missing bundle file"))?;
    let extract_dir =
        tempfile::TempDir::new().map_err(|err| ApiError::internal(err.to_string()))?;
    extract_zip(zip_file.path(), extract_dir.path()).map_err(ApiError::bad_request)?;
    let bundle_root = resolve_bundle_root(extract_dir.path())?;
    let result = resources
        .store
        .import_bundle(&bundle_root)
        .await
        .map_err(ApiError::internal)?;
    Ok(result)
}
