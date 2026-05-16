use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Extension, FromRequest, Multipart, State},
    http::{HeaderMap, Method, Request, StatusCode},
    middleware::{Next, from_fn_with_state},
    response::IntoResponse,
    routing::{any, get, post},
};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use tokio::sync::{Mutex, OnceCell, broadcast};
use tower_http::services::{ServeDir, ServeFile};

use crate::{TenantContext, TenantLayout, TenantResolver, validate_tenant_id};
use rulemorph_endpoint::{
    ApiMode, EndpointEngine, EngineConfig, RequestContext, validate_rules_dir,
};
use rulemorph_trace::{ImportResult, TraceStore, start_trace_watcher};

#[cfg(feature = "embedded-ui")]
use axum::extract::OriginalUri;
#[cfg(feature = "embedded-ui")]
use include_dir::{Dir, include_dir};

#[cfg(feature = "embedded-ui")]
static UI_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../rulemorph_ui/ui/dist");

mod api_key_routes;
mod auth;
mod import_zip;
mod rate_limit;
mod trace_routes;

use self::api_key_routes::{issue_api_key, list_api_keys, revoke_api_key, rotate_api_key};
#[cfg(test)]
use self::auth::pre_auth_rate_limit_key;
use self::auth::{
    DispatchInvalidApiKeyForRules, InternalApiRateLimitScope, api_rate_limit,
    apply_internal_tenant_context, apply_v1_auth_context, enforce_api_rate_limit,
    ensure_internal_auth, ensure_internal_auth_required, ensure_pre_auth_rate_limit_for_request,
    internal_auth, internal_auth_required, internal_tenant,
    maybe_apply_v1_auth_context_for_dispatch, pre_auth_rate_limit, rate_limit_key, v1_auth,
};
#[cfg(test)]
use self::import_zip::copy_zip_entry_bounded;
use self::import_zip::{extract_zip, resolve_bundle_root, validate_bundle_path};
pub use self::rate_limit::RateLimiter;
use self::trace_routes::{
    get_api_graph, get_trace, get_trace_finalize, get_trace_manifest, get_trace_nodes_chunk,
    get_trace_records_chunk, list_traces, stream_traces,
};

#[derive(Clone)]
pub enum UiSource {
    Filesystem(PathBuf),
    #[cfg(feature = "embedded-ui")]
    Embedded,
}

#[derive(Clone)]
pub struct TenantResources {
    pub tenant_id: String,
    pub data_dir: PathBuf,
    pub rules_dir: PathBuf,
    pub auth_dir: PathBuf,
    pub store: Arc<TraceStore>,
    pub api_engine: Option<Arc<EndpointEngine>>,
    pub trace_events: broadcast::Sender<()>,
}

#[derive(Clone)]
pub struct AppState {
    pub default_resources: Arc<TenantResources>,
    pub tenant_registry: Option<Arc<TenantRegistry>>,
    pub ui_source: Option<UiSource>,
    pub api_mode: ApiMode,
    pub tenant_resolver: Option<Arc<dyn TenantResolver>>,
    pub internal_api_key: Option<String>,
    pub allow_unauth_internal: bool,
    pub rate_limiter: Option<Arc<RateLimiter>>,
}

pub(crate) fn internal_auth_path_allowlist() -> Vec<String> {
    vec![
        "/internal/traces".to_string(),
        "/internal/traces/".to_string(),
        "/internal/api-graph".to_string(),
        "/internal/import".to_string(),
        "/internal/stream".to_string(),
    ]
}

pub struct TenantRegistry {
    base_dir: PathBuf,
    rules_dir: Option<PathBuf>,
    api_mode: ApiMode,
    ui_enabled: bool,
    port: u16,
    ssrf_allowlist: Vec<String>,
    ssrf_allow_private: bool,
    internal_api_key: Option<String>,
    tenants: Mutex<HashMap<String, Arc<OnceCell<Arc<TenantResources>>>>>,
}

const IMPORT_ZIP_MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
const IMPORT_ZIP_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const IMPORT_ZIP_MAX_ENTRIES: usize = 4096;

impl TenantRegistry {
    pub fn new(
        base_dir: PathBuf,
        rules_dir: Option<PathBuf>,
        api_mode: ApiMode,
        ui_enabled: bool,
        port: u16,
        ssrf_allowlist: Vec<String>,
        ssrf_allow_private: bool,
        internal_api_key: Option<String>,
    ) -> Self {
        Self {
            base_dir,
            rules_dir,
            api_mode,
            ui_enabled,
            port,
            ssrf_allowlist,
            ssrf_allow_private,
            internal_api_key,
            tenants: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_or_init(&self, tenant_id: &str) -> anyhow::Result<Arc<TenantResources>> {
        validate_tenant_id(tenant_id)?;
        let cell = {
            let mut guard = self.tenants.lock().await;
            guard
                .entry(tenant_id.to_string())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let resources = cell
            .get_or_try_init(|| async { self.init_resources(tenant_id).await })
            .await?;
        Ok(resources.clone())
    }

    async fn init_resources(&self, tenant_id: &str) -> anyhow::Result<Arc<TenantResources>> {
        let layout = TenantLayout::new(self.base_dir.clone(), tenant_id)?;
        tokio::fs::create_dir_all(layout.api_rules_dir()).await?;
        tokio::fs::create_dir_all(layout.auth_dir()).await?;

        let store = TraceStore::new(layout.data_dir()).await?;
        let (trace_events, _) = broadcast::channel(64);
        if self.ui_enabled {
            start_trace_watcher(layout.data_dir(), trace_events.clone());
        }

        let rules_dir = self.resolve_rules_dir(&layout);
        let api_engine = match self.api_mode {
            ApiMode::UiOnly => None,
            ApiMode::Rules => {
                if let Err(errs) = validate_rules_dir(&rules_dir) {
                    return Err(errs.into());
                }
                let internal_base = format!("http://localhost:{}", self.port);
                let mut config = EngineConfig::new(internal_base, layout.data_dir())
                    .with_ssrf_allowlist(self.ssrf_allowlist.clone())
                    .with_ssrf_allow_private(self.ssrf_allow_private)
                    .with_internal_auth_enabled(true)
                    .with_internal_auth_path_allowlist(internal_auth_path_allowlist());
                if let Some(internal_api_key) = self.internal_api_key.clone() {
                    config = config.with_internal_api_key(internal_api_key);
                }
                Some(Arc::new(EndpointEngine::load(rules_dir.clone(), config)?))
            }
        };

        Ok(Arc::new(TenantResources {
            tenant_id: tenant_id.to_string(),
            data_dir: layout.data_dir(),
            rules_dir,
            auth_dir: layout.auth_dir(),
            store: Arc::new(store),
            api_engine,
            trace_events,
        }))
    }

    fn resolve_rules_dir(&self, layout: &TenantLayout) -> PathBuf {
        match &self.rules_dir {
            Some(path) if path.is_absolute() => path.clone(),
            Some(path) => path.clone(),
            None => layout.api_rules_dir(),
        }
    }
}

pub fn build_router(state: AppState, ui_enabled: bool) -> Router {
    let api = match state.api_mode {
        ApiMode::UiOnly => {
            if ui_enabled {
                Router::new()
                    .route("/api/import", any(handle_api_import_only))
                    .route_layer(DefaultBodyLimit::max(IMPORT_ZIP_MAX_TOTAL_BYTES as usize))
            } else {
                Router::new()
            }
        }
        ApiMode::Rules => {
            let mut api_router = Router::new().route("/api/*path", any(handle_rules_api));
            let api_import = if ui_enabled {
                Router::new()
                    .route("/api/import", any(handle_api_import_or_rules))
                    .route_layer(DefaultBodyLimit::max(IMPORT_ZIP_MAX_TOTAL_BYTES as usize))
            } else {
                Router::new()
                    .route("/api/import", any(handle_api_import_or_rules_strict))
                    .route_layer(DefaultBodyLimit::max(IMPORT_ZIP_MAX_TOTAL_BYTES as usize))
            };
            if state.rate_limiter.is_some() {
                api_router = api_router.layer(from_fn_with_state(state.clone(), api_rate_limit));
            }
            if state.tenant_resolver.is_some() {
                api_router = api_router.layer(from_fn_with_state(state.clone(), v1_auth));
                if state.rate_limiter.is_some() {
                    api_router =
                        api_router.layer(from_fn_with_state(state.clone(), pre_auth_rate_limit));
                }
            }
            let api = Router::new().merge(api_import).merge(api_router);
            if state.tenant_resolver.is_some() {
                let mut v1 = Router::new().route("/v1/*path", any(handle_rules_api));
                if state.rate_limiter.is_some() {
                    v1 = v1.layer(from_fn_with_state(state.clone(), api_rate_limit));
                }
                v1 = v1.layer(from_fn_with_state(state.clone(), v1_auth));
                if state.rate_limiter.is_some() {
                    v1 = v1.layer(from_fn_with_state(state.clone(), pre_auth_rate_limit));
                }
                api.merge(v1)
            } else {
                api
            }
        }
    };

    let mut app = Router::new().merge(api);

    if ui_enabled {
        let mut internal = Router::new()
            .route("/internal/traces", get(list_traces))
            .route("/internal/traces/:id", get(get_trace))
            .route("/internal/traces/:id/manifest", get(get_trace_manifest))
            .route(
                "/internal/traces/:id/records/:chunk",
                get(get_trace_records_chunk),
            )
            .route(
                "/internal/traces/:id/nodes/:chunk",
                get(get_trace_nodes_chunk),
            )
            .route("/internal/traces/:id/finalize", get(get_trace_finalize))
            .route("/internal/stream", get(stream_traces))
            .route("/internal/api-graph", get(get_api_graph))
            .route("/internal/import", post(import_bundle_path));

        let mut internal_admin = Router::new()
            .route("/internal/api-keys", get(list_api_keys).post(issue_api_key))
            .route("/internal/api-keys/:id/revoke", post(revoke_api_key))
            .route("/internal/api-keys/:id/rotate", post(rotate_api_key));

        if state.rate_limiter.is_some() {
            internal = internal.layer(from_fn_with_state(state.clone(), api_rate_limit));
            internal_admin =
                internal_admin.layer(from_fn_with_state(state.clone(), api_rate_limit));
        }
        if state.tenant_resolver.is_some() {
            internal = internal.layer(from_fn_with_state(state.clone(), internal_tenant));
            internal_admin =
                internal_admin.layer(from_fn_with_state(state.clone(), internal_tenant));
        }
        if !state.allow_unauth_internal
            || state.internal_api_key.is_some()
            || state.tenant_resolver.is_some()
        {
            internal = internal.layer(from_fn_with_state(state.clone(), internal_auth));
        }
        internal_admin =
            internal_admin.layer(from_fn_with_state(state.clone(), internal_auth_required));
        if state.rate_limiter.is_some() {
            internal = internal.layer(from_fn_with_state(state.clone(), pre_auth_rate_limit));
            internal_admin =
                internal_admin.layer(from_fn_with_state(state.clone(), pre_auth_rate_limit));
        }

        app = app.merge(internal);
        app = app.merge(internal_admin);
        if let Some(ui_source) = state.ui_source.clone() {
            app = match ui_source {
                UiSource::Filesystem(dir) => {
                    let static_service =
                        ServeDir::new(dir.clone()).fallback(ServeFile::new(dir.join("index.html")));
                    app.fallback_service(static_service)
                }
                #[cfg(feature = "embedded-ui")]
                UiSource::Embedded => app.fallback(serve_embedded_ui),
            };
        }
    }

    app.layer(from_fn_with_state(state.clone(), inject_default_resources))
        .with_state(state)
}

#[cfg(feature = "embedded-ui")]
async fn serve_embedded_ui(OriginalUri(uri): OriginalUri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();
    if path.is_empty() {
        path = "index.html".to_string();
    }

    if let Some(file) = UI_DIR.get_file(&path) {
        return embedded_response(file.path().to_str(), file.contents());
    }

    if let Some(index) = UI_DIR.get_file("index.html") {
        return embedded_response(Some("index.html"), index.contents());
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "embedded ui missing index.html",
    )
        .into_response()
}

#[cfg(feature = "embedded-ui")]
fn embedded_response(path: Option<&str>, contents: &'static [u8]) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    let mime = match path {
        Some(path) => mime_guess::from_path(path).first_or_octet_stream(),
        None => mime_guess::mime::APPLICATION_OCTET_STREAM,
    };
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        mime.as_ref()
            .parse()
            .unwrap_or_else(|_| axum::http::HeaderValue::from_static("application/octet-stream")),
    );
    (headers, contents).into_response()
}

fn request_resources(
    state: &AppState,
    request: &Request<axum::body::Body>,
) -> Arc<TenantResources> {
    request
        .extensions()
        .get::<Arc<TenantResources>>()
        .cloned()
        .unwrap_or_else(|| state.default_resources.clone())
}

fn request_engine<'a>(
    state: &'a AppState,
    resources: &'a Arc<TenantResources>,
) -> std::result::Result<&'a Arc<EndpointEngine>, ApiError> {
    resources
        .api_engine
        .as_ref()
        .or(state.default_resources.api_engine.as_ref())
        .ok_or_else(|| ApiError::internal("api engine not configured"))
}

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

async fn handle_api_import_only(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    if !is_zip_import_request(&request) {
        return Err(ApiError::not_found("no endpoint matched"));
    }
    run_api_import_request(&state, request, false).await
}

async fn handle_api_import_or_rules(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    handle_api_import_or_rules_with_auth_mode(state, request, false).await
}

async fn handle_api_import_or_rules_strict(
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

async fn run_rules_api_request(
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

async fn handle_rules_api(
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
    if engine.allows_internal_auth() {
        if let Some(internal_api_key) = state.internal_api_key.clone() {
            request_context.internal_api_key = Some(internal_api_key);
        }
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

async fn inject_default_resources(
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

#[derive(Deserialize)]
struct ImportPathRequest {
    bundle_path: String,
}

async fn import_bundle_path(
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

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn internal(err: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: err.to_string(),
        }
    }

    fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    fn too_many_requests(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum::extract::ConnectInfo;
    use axum::http::{HeaderValue, Request, header::AUTHORIZATION};

    use crate::{TenantContext, TenantResolver};
    use crate::{TenantLayout, TenantRegistry};

    use super::{
        ApiMode, AppState, IMPORT_ZIP_MAX_ENTRIES, TenantResources, TraceStore, build_router,
        copy_zip_entry_bounded, extract_zip, pre_auth_rate_limit_key,
    };

    struct StaticTenantResolver;

    #[async_trait]
    impl TenantResolver for StaticTenantResolver {
        async fn resolve(&self, _api_key: &str) -> anyhow::Result<Option<TenantContext>> {
            Ok(Some(TenantContext::new("tenant-a")))
        }
    }

    #[test]
    fn pre_auth_rate_limit_uses_api_key_when_present() {
        let mut request = Request::builder()
            .uri("/v1/traces")
            .body(axum::body::Body::empty())
            .expect("request");
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer rmk_tenant-a.secret"),
        );
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 3000))));

        let key = pre_auth_rate_limit_key(&request);
        assert!(key.starts_with("preauth:key:"));
        assert!(!key.contains("rmk_tenant-a.secret"));
    }

    #[test]
    fn pre_auth_rate_limit_separates_distinct_api_keys() {
        let mut first = Request::builder()
            .uri("/v1/traces")
            .body(axum::body::Body::empty())
            .expect("request");
        first
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer key-a"));
        first
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 3000))));
        let mut second = Request::builder()
            .uri("/v1/traces")
            .body(axum::body::Body::empty())
            .expect("request");
        second
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer key-b"));
        second
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 3000))));

        assert_ne!(
            pre_auth_rate_limit_key(&first),
            pre_auth_rate_limit_key(&second)
        );
    }

    #[test]
    fn pre_auth_rate_limit_falls_back_to_ip_without_api_key() {
        let mut request = Request::builder()
            .uri("/v1/traces")
            .body(axum::body::Body::empty())
            .expect("request");
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 3000))));

        let key = pre_auth_rate_limit_key(&request);
        assert_eq!(key, "preauth:ip:127.0.0.1");
    }

    #[test]
    fn tenant_registry_keeps_configured_relative_rules_dir_relative_to_cwd() {
        let registry = TenantRegistry::new(
            PathBuf::from("/tmp/rulemorph-data"),
            Some(PathBuf::from("./assets/api_rules")),
            ApiMode::Rules,
            false,
            8080,
            Vec::new(),
            false,
            None,
        );
        let layout = TenantLayout::new(PathBuf::from("/tmp/rulemorph-data"), "tenant-a")
            .expect("tenant layout");

        assert_eq!(
            registry.resolve_rules_dir(&layout),
            PathBuf::from("./assets/api_rules")
        );
    }

    #[test]
    fn zip_copy_stops_after_file_limit() {
        let mut input = std::io::Cursor::new(vec![b'x'; 12]);
        let mut output = Vec::new();
        let copied = copy_zip_entry_bounded(&mut input, &mut output, 8).expect("copy");
        assert_eq!(copied, 12);
        assert!(output.len() <= 8);
    }

    #[test]
    fn zip_extract_rejects_too_many_entries() {
        let temp = tempfile::tempdir().expect("tempdir");
        let zip_path = temp.path().join("bundle.zip");
        let file = std::fs::File::create(&zip_path).expect("create zip");
        let mut zip_writer = zip::ZipWriter::new(file);
        let options =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        for index in 0..=IMPORT_ZIP_MAX_ENTRIES {
            zip_writer
                .start_file(format!("traces/{index}/trace.json"), options)
                .expect("start file");
        }
        zip_writer.finish().expect("finish zip");

        let err = extract_zip(&zip_path, temp.path().join("out").as_path())
            .expect_err("zip should be rejected");
        assert!(err.contains("too many entries"));
    }

    #[tokio::test]
    async fn unauth_internal_is_not_allowed_with_tenant_resolver() {
        let temp = tempfile::tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let store = TraceStore::new(data_dir.clone())
            .await
            .expect("trace store");
        let (trace_events, _) = tokio::sync::broadcast::channel(16);
        let auth_dir = data_dir.join("auth");
        tokio::fs::create_dir_all(&auth_dir)
            .await
            .expect("create auth dir");
        let resources = TenantResources {
            tenant_id: "default".to_string(),
            data_dir: data_dir.clone(),
            rules_dir: data_dir.join("api_rules"),
            auth_dir,
            store: Arc::new(store),
            api_engine: None,
            trace_events,
        };
        let state = AppState {
            default_resources: Arc::new(resources),
            tenant_registry: None,
            ui_source: None,
            api_mode: ApiMode::UiOnly,
            tenant_resolver: Some(Arc::new(StaticTenantResolver)),
            internal_api_key: None,
            allow_unauth_internal: true,
            rate_limiter: None,
        };
        let app = build_router(state, true);
        let response = tower::ServiceExt::oneshot(
            app,
            Request::builder()
                .uri("/internal/traces")
                .header("x-tenant-id", "tenant-a")
                .body(axum::body::Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
