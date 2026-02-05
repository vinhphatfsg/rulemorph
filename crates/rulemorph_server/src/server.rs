use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{ConnectInfo, Path as AxumPath, State},
    http::{HeaderMap, Request, StatusCode},
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
use tokio::sync::{Mutex, broadcast};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tower_http::services::{ServeDir, ServeFile};

use crate::api_graph::{ApiGraphResponse, build_api_graph};
use crate::{TenantContext, TenantResolver};
use rulemorph_endpoint::{ApiMode, EndpointEngine};
use rulemorph_trace::{ImportResult, TraceManifest, TraceMeta, TraceNodeChunkEntry, TraceStore};

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
pub struct AppState {
    pub store: Arc<TraceStore>,
    pub ui_source: Option<UiSource>,
    pub api_mode: ApiMode,
    pub api_engine: Option<Arc<EndpointEngine>>,
    pub trace_events: broadcast::Sender<()>,
    pub tenant_resolver: Option<Arc<dyn TenantResolver>>,
    pub internal_api_key: Option<String>,
    pub allow_unauth_internal: bool,
    pub rate_limiter: Option<Arc<RateLimiter>>,
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

pub fn build_router(state: AppState, ui_enabled: bool) -> Router {
    let api = match state.api_mode {
        ApiMode::UiOnly => Router::new(),
        ApiMode::Rules => {
            let mut api_router = Router::new().route("/api/*path", any(handle_rules_api));
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
            Router::new().merge(api_router).merge(v1)
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

        if state.rate_limiter.is_some() {
            internal = internal.layer(from_fn_with_state(state.clone(), api_rate_limit));
        }
        if !state.allow_unauth_internal
            || state.internal_api_key.is_some()
            || state.tenant_resolver.is_some()
        {
            internal = internal.layer(from_fn_with_state(state.clone(), internal_auth));
        }
        if state.rate_limiter.is_some() {
            internal = internal.layer(from_fn_with_state(state.clone(), pre_auth_rate_limit));
        }

        let ui_source = match state.ui_source.clone() {
            Some(source) => source,
            None => {
                return app.merge(internal).with_state(state);
            }
        };

        app = app.merge(internal);
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

    app.with_state(state)
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

async fn handle_rules_api(
    state: State<AppState>,
    request: axum::http::Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    let state = state.0;
    let engine = state
        .api_engine
        .as_ref()
        .ok_or_else(|| ApiError::internal("api engine not configured"))?;
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

async fn v1_auth(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    let resolver = state
        .tenant_resolver
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("tenant resolver not configured"))?;
    let api_key = extract_api_key(request.headers())
        .ok_or_else(|| ApiError::unauthorized("missing api key"))?;
    let context = resolver
        .resolve(&api_key)
        .await
        .map_err(ApiError::internal)?;
    let Some(context) = context else {
        return Err(ApiError::unauthorized("invalid api key"));
    };
    request.extensions_mut().insert(context);
    Ok(next.run(request).await)
}

async fn internal_auth(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    let Some(expected) = state.internal_api_key.as_deref() else {
        if state.allow_unauth_internal {
            return Ok(next.run(request).await);
        }
        return Err(ApiError::service_unavailable(
            "internal api key not configured",
        ));
    };
    let provided = extract_api_key(request.headers())
        .ok_or_else(|| ApiError::unauthorized("missing internal api key"))?;
    if provided != expected {
        return Err(ApiError::unauthorized("invalid internal api key"));
    }
    Ok(next.run(request).await)
}

async fn pre_auth_rate_limit(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    if let Some(limiter) = state.rate_limiter.as_ref() {
        let key = pre_auth_rate_limit_key(&request);
        if !limiter.allow(&key).await {
            return Err(ApiError::too_many_requests("rate limit exceeded"));
        }
    }
    Ok(next.run(request).await)
}

fn pre_auth_rate_limit_key(request: &Request<axum::body::Body>) -> String {
    if let Some(ConnectInfo(addr)) = request
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
    {
        return format!("preauth:ip:{}", addr.ip());
    }
    if let Some(api_key) = extract_api_key(request.headers()) {
        return format!("preauth:key:{:x}", hash_string(&api_key));
    }
    "preauth:anonymous".to_string()
}

fn hash_string(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

async fn api_rate_limit(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    if let Some(limiter) = state.rate_limiter.as_ref() {
        let key = rate_limit_key(&state, &request);
        if !limiter.allow(&key).await {
            return Err(ApiError::too_many_requests("rate limit exceeded"));
        }
    }
    Ok(next.run(request).await)
}

fn rate_limit_key(state: &AppState, request: &Request<axum::body::Body>) -> String {
    if let Some(context) = request.extensions().get::<TenantContext>() {
        return format!("tenant:{}", context.tenant_id);
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
    state: State<AppState>,
) -> std::result::Result<Json<TraceListResponse>, ApiError> {
    let state = state.0;
    let mut traces = state.store.list().await.map_err(ApiError::internal)?;
    if traces.is_empty() {
        state
            .store
            .seed_sample()
            .await
            .map_err(ApiError::internal)?;
        traces = state.store.list().await.map_err(ApiError::internal)?;
    }
    Ok(Json(TraceListResponse { traces }))
}

async fn get_trace(
    state: State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let state = state.0;
    let trace = state.store.get(&id).await.map_err(ApiError::internal)?;
    match trace {
        Some(value) => Ok(Json(json!({ "trace": value }))),
        None => Err(ApiError::not_found("trace not found")),
    }
}

async fn get_trace_manifest(
    state: State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> std::result::Result<Json<TraceManifestResponse>, ApiError> {
    let state = state.0;
    let manifest = state
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
    state: State<AppState>,
    AxumPath(path): AxumPath<TraceChunkPath>,
) -> std::result::Result<Json<TraceRecordsResponse>, ApiError> {
    let state = state.0;
    let records = state
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
    state: State<AppState>,
    AxumPath(path): AxumPath<TraceChunkPath>,
) -> std::result::Result<Json<TraceNodesResponse>, ApiError> {
    let state = state.0;
    let nodes = state
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
    state: State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> std::result::Result<Json<TraceFinalizeResponse>, ApiError> {
    let state = state.0;
    let finalize = state
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

async fn import_bundle_path(
    state: State<AppState>,
    Json(payload): Json<ImportPathRequest>,
) -> std::result::Result<Json<ImportResult>, ApiError> {
    let state = state.0;
    let bundle_path = validate_bundle_path(&PathBuf::from(payload.bundle_path))?;
    let result = state
        .store
        .import_bundle(&bundle_path)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(result))
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

async fn stream_traces(
    state: State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream =
        BroadcastStream::new(state.trace_events.subscribe()).filter_map(|message| match message {
            Ok(_) => Some(Ok(Event::default().event("traces").data("updated"))),
            Err(_) => None,
        });
    Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn get_api_graph(
    state: State<AppState>,
) -> std::result::Result<Json<ApiGraphResponse>, ApiError> {
    let state = state.0;
    let graph = build_api_graph(state.store.data_dir()).map_err(ApiError::internal)?;
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
