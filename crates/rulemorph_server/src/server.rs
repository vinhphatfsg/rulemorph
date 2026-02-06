use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use axum::{
    Json, Router,
    extract::{ConnectInfo, Extension, FromRequest, Multipart, Path as AxumPath, State},
    http::{HeaderMap, Method, Request, StatusCode},
    middleware::{Next, from_fn_with_state},
    response::{
        IntoResponse,
        sse::{Event, Sse},
    },
    routing::{any, get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OnceCell, broadcast};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tower_http::services::{ServeDir, ServeFile};
use zip::ZipArchive;

use crate::api_graph::{ApiGraphResponse, build_api_graph};
use crate::{ApiKeyInfo, ApiKeyIssueResult, ApiKeyStore};
use crate::{TenantContext, TenantLayout, TenantResolver, validate_tenant_id};
use rulemorph_endpoint::{
    ApiMode, EndpointEngine, EngineConfig, RequestContext, validate_rules_dir,
};
use rulemorph_trace::{
    ImportResult, TraceManifest, TraceMeta, TraceNodeChunkEntry, TraceStore, start_trace_watcher,
};

#[cfg(feature = "embedded-ui")]
use axum::{extract::OriginalUri, http::HeaderMap};
#[cfg(feature = "embedded-ui")]
use include_dir::{Dir, include_dir};

#[cfg(feature = "embedded-ui")]
static UI_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../rulemorph_ui/ui/dist");

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

#[derive(Debug)]
pub struct RateLimiter {
    limit: u64,
    state: Mutex<HashMap<String, RateLimitState>>,
    max_entries: usize,
    ttl: Duration,
}

#[derive(Debug)]
struct RateLimitState {
    window_start: Instant,
    count: u64,
    last_seen: Instant,
}

const IMPORT_ZIP_MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
const IMPORT_ZIP_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
static API_KEY_FILE_LOCKS: OnceLock<StdMutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

fn api_key_file_lock(path: &Path) -> Arc<Mutex<()>> {
    let lock_map = API_KEY_FILE_LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = lock_map
        .lock()
        .expect("api key lock map should not be poisoned");
    guard
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

impl RateLimiter {
    pub fn new(limit: u64) -> Self {
        Self {
            limit,
            state: Mutex::new(HashMap::new()),
            max_entries: 10_000,
            ttl: Duration::from_secs(600),
        }
    }

    pub async fn allow(&self, key: &str) -> bool {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        if state.len() >= self.max_entries {
            state.retain(|_, entry| now.duration_since(entry.last_seen) <= self.ttl);
            if state.len() >= self.max_entries {
                if let Some((oldest_key, _)) = state.iter().min_by_key(|(_, entry)| entry.last_seen)
                {
                    let oldest_key = oldest_key.clone();
                    state.remove(&oldest_key);
                }
            }
        }

        let entry = state.entry(key.to_string()).or_insert(RateLimitState {
            window_start: now,
            count: 0,
            last_seen: now,
        });
        entry.last_seen = now;
        if now.duration_since(entry.window_start) >= Duration::from_secs(1) {
            entry.window_start = now;
            entry.count = 0;
        }
        if entry.count >= self.limit {
            return false;
        }
        entry.count += 1;
        true
    }
}

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
                let allow_internal_auth = self
                    .rules_dir
                    .as_ref()
                    .map(|path| path.is_absolute())
                    .unwrap_or(false);
                let mut config = EngineConfig::new(internal_base, layout.data_dir())
                    .with_ssrf_allowlist(self.ssrf_allowlist.clone())
                    .with_ssrf_allow_private(self.ssrf_allow_private)
                    .with_internal_auth_enabled(allow_internal_auth);
                if allow_internal_auth {
                    config =
                        config.with_internal_auth_path_allowlist(internal_auth_path_allowlist());
                    if let Some(internal_api_key) = self.internal_api_key.clone() {
                        config = config.with_internal_api_key(internal_api_key);
                    }
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
            Some(path) => layout.data_dir().join(path),
            None => layout.api_rules_dir(),
        }
    }
}

pub fn build_router(state: AppState, ui_enabled: bool) -> Router {
    let api = match state.api_mode {
        ApiMode::UiOnly => {
            if ui_enabled {
                Router::new().route("/api/import", any(handle_api_import_only))
            } else {
                Router::new()
            }
        }
        ApiMode::Rules => {
            let mut api_router = Router::new().route("/api/*path", any(handle_rules_api));
            let api_import = if ui_enabled {
                Router::new().route("/api/import", any(handle_api_import_or_rules))
            } else {
                Router::new().route("/api/import", any(handle_api_import_or_rules_strict))
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
            let mut v1 = Router::new().route("/v1/*path", any(handle_rules_api));
            if state.rate_limiter.is_some() {
                v1 = v1.layer(from_fn_with_state(state.clone(), api_rate_limit));
            }
            v1 = v1.layer(from_fn_with_state(state.clone(), v1_auth));
            if state.rate_limiter.is_some() {
                v1 = v1.layer(from_fn_with_state(state.clone(), pre_auth_rate_limit));
            }
            Router::new().merge(api_import).merge(api_router).merge(v1)
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

fn has_zip_import_hint(headers: &HeaderMap) -> bool {
    headers
        .get("x-rulemorph-import")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("zip"))
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
    if has_zip_import_hint(request.headers()) {
        return run_api_import_request(&state, request, require_internal_key).await;
    }
    if state.tenant_resolver.is_some() && state.rate_limiter.is_some() {
        ensure_pre_auth_rate_limit_for_request(&state, &mut request).await?;
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

async fn apply_v1_auth_context(
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

async fn maybe_apply_v1_auth_context_for_dispatch(
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

async fn v1_auth(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    apply_v1_auth_context(&state, &mut request).await?;
    Ok(next.run(request).await)
}

async fn apply_internal_tenant_context(
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

async fn internal_tenant(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    apply_internal_tenant_context(&state, &mut request).await?;
    Ok(next.run(request).await)
}

fn ensure_internal_auth(
    state: &AppState,
    headers: &HeaderMap,
) -> std::result::Result<(), ApiError> {
    let Some(expected) = state.internal_api_key.as_deref() else {
        if state.allow_unauth_internal {
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

fn ensure_internal_auth_required(
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

async fn internal_auth(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    ensure_internal_auth(&state, request.headers())?;
    Ok(next.run(request).await)
}

async fn internal_auth_required(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    ensure_internal_auth_required(&state, request.headers())?;
    Ok(next.run(request).await)
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

#[derive(Clone, Copy, Debug)]
struct PreAuthRateLimitApplied;

#[derive(Clone, Copy, Debug)]
struct InternalApiRateLimitScope;

#[derive(Clone, Copy, Debug)]
struct DispatchInvalidApiKeyForRules;

async fn ensure_pre_auth_rate_limit_for_request(
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

async fn pre_auth_rate_limit(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    ensure_pre_auth_rate_limit_for_request(&state, &mut request).await?;
    Ok(next.run(request).await)
}

fn pre_auth_rate_limit_key(request: &Request<axum::body::Body>) -> String {
    if let Some(api_key) = extract_api_key(request.headers()) {
        return format!("preauth:key:{:x}", hash_string(&api_key));
    }
    if let Some(ConnectInfo(addr)) = request
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
    {
        return format!("preauth:ip:{}", addr.ip());
    }
    "preauth:anonymous".to_string()
}

fn hash_string(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

async fn enforce_api_rate_limit(
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

async fn api_rate_limit(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    let key = rate_limit_key(&state, &request);
    enforce_api_rate_limit(&state, key).await?;
    Ok(next.run(request).await)
}

fn rate_limit_key(state: &AppState, request: &Request<axum::body::Body>) -> String {
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

#[derive(Serialize)]
struct TraceListResponse {
    traces: Vec<TraceMeta>,
}

#[derive(Serialize)]
struct TraceManifestResponse {
    manifest: TraceManifest,
}

#[derive(Serialize)]
struct TraceRecordsResponse {
    records: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct TraceNodesResponse {
    nodes: Vec<TraceNodeChunkEntry>,
}

#[derive(Serialize)]
struct TraceFinalizeResponse {
    finalize: serde_json::Value,
}

#[derive(Deserialize)]
struct TraceChunkPath {
    id: String,
    chunk: usize,
}

async fn list_traces(
    Extension(resources): Extension<Arc<TenantResources>>,
) -> std::result::Result<Json<TraceListResponse>, ApiError> {
    let mut traces = resources.store.list().await.map_err(ApiError::internal)?;
    if traces.is_empty() {
        resources
            .store
            .seed_sample()
            .await
            .map_err(ApiError::internal)?;
        traces = resources.store.list().await.map_err(ApiError::internal)?;
    }
    Ok(Json(TraceListResponse { traces }))
}

async fn get_trace(
    Extension(resources): Extension<Arc<TenantResources>>,
    AxumPath(id): AxumPath<String>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let trace = resources.store.get(&id).await.map_err(ApiError::internal)?;
    match trace {
        Some(value) => Ok(Json(json!({ "trace": value }))),
        None => Err(ApiError::not_found("trace not found")),
    }
}

async fn get_trace_manifest(
    Extension(resources): Extension<Arc<TenantResources>>,
    AxumPath(id): AxumPath<String>,
) -> std::result::Result<Json<TraceManifestResponse>, ApiError> {
    let manifest = resources
        .store
        .get_manifest(&id)
        .await
        .map_err(ApiError::internal)?;
    match manifest {
        Some(manifest) => Ok(Json(TraceManifestResponse { manifest })),
        None => Err(ApiError::not_found("trace manifest not found")),
    }
}

async fn get_trace_records_chunk(
    Extension(resources): Extension<Arc<TenantResources>>,
    AxumPath(path): AxumPath<TraceChunkPath>,
) -> std::result::Result<Json<TraceRecordsResponse>, ApiError> {
    let records = resources
        .store
        .get_records_chunk(&path.id, path.chunk)
        .await
        .map_err(ApiError::internal)?;
    match records {
        Some(records) => Ok(Json(TraceRecordsResponse { records })),
        None => Err(ApiError::not_found("trace records chunk not found")),
    }
}

async fn get_trace_nodes_chunk(
    Extension(resources): Extension<Arc<TenantResources>>,
    AxumPath(path): AxumPath<TraceChunkPath>,
) -> std::result::Result<Json<TraceNodesResponse>, ApiError> {
    let nodes = resources
        .store
        .get_nodes_chunk(&path.id, path.chunk)
        .await
        .map_err(ApiError::internal)?;
    match nodes {
        Some(nodes) => Ok(Json(TraceNodesResponse { nodes })),
        None => Err(ApiError::not_found("trace nodes chunk not found")),
    }
}

async fn get_trace_finalize(
    Extension(resources): Extension<Arc<TenantResources>>,
    AxumPath(id): AxumPath<String>,
) -> std::result::Result<Json<TraceFinalizeResponse>, ApiError> {
    let finalize = resources
        .store
        .get_finalize_chunk(&id)
        .await
        .map_err(ApiError::internal)?;
    match finalize {
        Some(finalize) => Ok(Json(TraceFinalizeResponse { finalize })),
        None => Err(ApiError::not_found("trace finalize not found")),
    }
}

#[derive(Deserialize)]
struct ImportPathRequest {
    bundle_path: String,
}

#[derive(Deserialize)]
struct ApiKeyIssueRequest {
    label: Option<String>,
}

#[derive(Deserialize)]
struct ApiKeyRotateRequest {
    label: Option<String>,
}

#[derive(Serialize)]
struct ApiKeyListResponse {
    keys: Vec<ApiKeyInfo>,
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

async fn list_api_keys(
    Extension(resources): Extension<Arc<TenantResources>>,
) -> std::result::Result<Json<ApiKeyListResponse>, ApiError> {
    let path = resources.auth_dir.join("api_keys.json");
    let store = ApiKeyStore::load(path, &resources.tenant_id).map_err(ApiError::internal)?;
    let keys = store.map(|store| store.list()).unwrap_or_default();
    Ok(Json(ApiKeyListResponse { keys }))
}

async fn issue_api_key(
    Extension(resources): Extension<Arc<TenantResources>>,
    Json(payload): Json<ApiKeyIssueRequest>,
) -> std::result::Result<Json<ApiKeyIssueResult>, ApiError> {
    let path = resources.auth_dir.join("api_keys.json");
    let lock = api_key_file_lock(&path);
    let _guard = lock.lock().await;
    let mut store =
        ApiKeyStore::load_or_init(path, &resources.tenant_id).map_err(ApiError::internal)?;
    let issued = store.issue(payload.label).map_err(ApiError::internal)?;
    Ok(Json(issued))
}

async fn revoke_api_key(
    Extension(resources): Extension<Arc<TenantResources>>,
    AxumPath(id): AxumPath<String>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let path = resources.auth_dir.join("api_keys.json");
    let lock = api_key_file_lock(&path);
    let _guard = lock.lock().await;
    let mut store = ApiKeyStore::load(path, &resources.tenant_id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("api key store not found"))?;
    let revoked = store.revoke(&id).map_err(ApiError::internal)?;
    Ok(Json(json!({ "revoked": revoked })))
}

async fn rotate_api_key(
    Extension(resources): Extension<Arc<TenantResources>>,
    AxumPath(id): AxumPath<String>,
    Json(payload): Json<ApiKeyRotateRequest>,
) -> std::result::Result<Json<ApiKeyIssueResult>, ApiError> {
    let path = resources.auth_dir.join("api_keys.json");
    let lock = api_key_file_lock(&path);
    let _guard = lock.lock().await;
    let mut store =
        ApiKeyStore::load_or_init(path, &resources.tenant_id).map_err(ApiError::internal)?;
    let issued = store
        .rotate(&id, payload.label)
        .map_err(ApiError::internal)?;
    let Some(issued) = issued else {
        return Err(ApiError::not_found("api key not found"));
    };
    Ok(Json(issued))
}

fn validate_bundle_path(bundle_path: &Path) -> std::result::Result<PathBuf, ApiError> {
    let bundle_path = bundle_path
        .canonicalize()
        .map_err(|err| ApiError::bad_request(format!("invalid bundle_path: {}", err)))?;
    if !bundle_path.is_dir() {
        return Err(ApiError::bad_request("bundle_path must be a directory"));
    }
    let temp_dir = std::env::temp_dir()
        .canonicalize()
        .unwrap_or_else(|_| std::env::temp_dir());
    if !bundle_path.starts_with(&temp_dir) {
        return Err(ApiError::bad_request(format!(
            "bundle_path must be under {}",
            temp_dir.display()
        )));
    }
    Ok(bundle_path)
}

fn extract_zip(path: &Path, dest: &Path) -> Result<(), String> {
    let file = File::open(path).map_err(|err| format!("failed to open zip: {}", err))?;
    let mut archive = ZipArchive::new(file).map_err(|err| format!("invalid zip: {}", err))?;
    let mut total_bytes: u64 = 0;

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|err| format!("zip entry error: {}", err))?;
        let name = entry.name().to_string();
        let entry_path = Path::new(&name);
        for component in entry_path.components() {
            match component {
                std::path::Component::Normal(_) => {}
                _ => {
                    return Err(format!("invalid zip entry path: {}", name));
                }
            }
        }
        if let Some(mode) = entry.unix_mode() {
            if (mode & 0o170000) == 0o120000 {
                return Err(format!("zip entry is symlink: {}", name));
            }
        }
        let out_path = dest.join(entry_path);
        if entry.is_dir() {
            std::fs::create_dir_all(&out_path)
                .map_err(|err| format!("failed to create dir: {}", err))?;
            continue;
        }
        let size = entry.size();
        if size > IMPORT_ZIP_MAX_FILE_BYTES {
            return Err(format!("zip entry too large: {}", name));
        }
        total_bytes = total_bytes.saturating_add(size);
        if total_bytes > IMPORT_ZIP_MAX_TOTAL_BYTES {
            return Err("zip exceeds max total bytes".to_string());
        }
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create dir: {}", err))?;
        }
        let mut outfile = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&out_path)
            .map_err(|err| format!("failed to create file: {}", err))?;
        std::io::copy(&mut entry, &mut outfile)
            .map_err(|err| format!("failed to write file: {}", err))?;
    }
    Ok(())
}

fn resolve_bundle_root(base: &Path) -> std::result::Result<PathBuf, ApiError> {
    if base.join("traces").exists() || base.join("rules").exists() {
        return Ok(base.to_path_buf());
    }
    let mut entries = std::fs::read_dir(base)
        .map_err(|err| ApiError::bad_request(format!("invalid zip bundle: {}", err)))?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>();
    if entries.len() == 1 {
        let entry = entries.remove(0);
        let path = entry.path();
        if path.is_dir() && (path.join("traces").exists() || path.join("rules").exists()) {
            return Ok(path);
        }
    }
    Err(ApiError::bad_request(
        "zip bundle must include traces/ or rules/",
    ))
}

async fn stream_traces(
    Extension(resources): Extension<Arc<TenantResources>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream =
        BroadcastStream::new(resources.trace_events.subscribe()).filter_map(
            |message| match message {
                Ok(_) => Some(Ok(Event::default().event("traces").data("updated"))),
                Err(_) => None,
            },
        );
    Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn get_api_graph(
    Extension(resources): Extension<Arc<TenantResources>>,
) -> std::result::Result<Json<ApiGraphResponse>, ApiError> {
    let graph = build_api_graph(&resources.rules_dir).map_err(ApiError::internal)?;
    Ok(Json(graph))
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

    use axum::extract::ConnectInfo;
    use axum::http::{HeaderValue, Request, header::AUTHORIZATION};

    use super::pre_auth_rate_limit_key;

    #[test]
    fn pre_auth_rate_limit_uses_api_key_even_with_connect_info() {
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
}
