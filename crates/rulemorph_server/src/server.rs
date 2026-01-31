use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{
        IntoResponse,
        sse::{Event, Sse},
    },
    routing::{any, get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tower_http::services::{ServeDir, ServeFile};

use crate::api_graph::{ApiGraphResponse, build_api_graph};
use rulemorph_endpoint::{ApiMode, EndpointEngine};
use rulemorph_trace::{ImportResult, TraceMeta, TraceStore};

#[cfg(feature = "embedded-ui")]
use axum::{extract::OriginalUri, http::HeaderMap};
#[cfg(feature = "embedded-ui")]
use include_dir::{Dir, include_dir};

#[cfg(feature = "embedded-ui")]
static UI_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../rulemorph_ui/ui/dist");

#[derive(Clone)]
pub(crate) enum UiSource {
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
}

pub fn build_router(state: AppState, ui_enabled: bool) -> Router {
    let api = match state.api_mode {
        ApiMode::UiOnly => Router::new(),
        ApiMode::Rules => Router::new().route("/api/*path", any(handle_rules_api)),
    };

    let mut app = Router::new().merge(api);

    if ui_enabled {
        let internal = Router::new()
            .route("/internal/traces", get(list_traces))
            .route("/internal/traces/:id", get(get_trace))
            .route("/internal/stream", get(stream_traces))
            .route("/internal/api-graph", get(get_api_graph))
            .route("/internal/import", post(import_bundle_path));

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

#[derive(Serialize)]
struct TraceListResponse {
    traces: Vec<TraceMeta>,
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

#[derive(Deserialize)]
struct ImportPathRequest {
    bundle_path: String,
}

async fn import_bundle_path(
    state: State<AppState>,
    Json(payload): Json<ImportPathRequest>,
) -> std::result::Result<Json<ImportResult>, ApiError> {
    let state = state.0;
    let bundle_path = PathBuf::from(payload.bundle_path);
    let result = state
        .store
        .import_bundle(&bundle_path)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(result))
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
