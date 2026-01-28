use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{sse::{Event, Sse}, IntoResponse},
    routing::{any, get, post},
    Json, Router,
};
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::services::{ServeDir, ServeFile};

use crate::endpoint_engine::{ApiMode, EndpointEngine};
use crate::api_graph::{build_api_graph, ApiGraphResponse};
use crate::trace_store::{ImportResult, TraceMeta, TraceStore};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<TraceStore>,
    pub ui_dir: PathBuf,
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

        let static_service = ServeDir::new(state.ui_dir.clone())
            .fallback(ServeFile::new(state.ui_dir.join("index.html")));

        app = app.merge(internal).fallback_service(static_service);
    }

    app.with_state(state)
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
        state.store.seed_sample().await.map_err(ApiError::internal)?;
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
    let stream = BroadcastStream::new(state.trace_events.subscribe()).filter_map(|message| {
        match message {
            Ok(_) => Some(Ok(Event::default().event("traces").data("updated"))),
            Err(_) => None,
        }
    });
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(15)))
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
