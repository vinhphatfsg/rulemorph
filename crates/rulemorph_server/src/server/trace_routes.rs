use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json,
    extract::{Extension, Path as AxumPath},
    response::sse::{Event, Sse},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_stream::{StreamExt, wrappers::BroadcastStream};

use crate::api_graph::{ApiGraphResponse, build_api_graph};
use rulemorph_trace::{TraceManifest, TraceMeta, TraceNodeChunkEntry};

use super::{ApiError, TenantResources};

#[derive(Serialize)]
pub(super) struct TraceListResponse {
    traces: Vec<TraceMeta>,
}

#[derive(Serialize)]
pub(super) struct TraceManifestResponse {
    manifest: TraceManifest,
}

#[derive(Serialize)]
pub(super) struct TraceRecordsResponse {
    records: Vec<serde_json::Value>,
}

#[derive(Serialize)]
pub(super) struct TraceNodesResponse {
    nodes: Vec<TraceNodeChunkEntry>,
}

#[derive(Serialize)]
pub(super) struct TraceFinalizeResponse {
    finalize: serde_json::Value,
}

#[derive(Deserialize)]
pub(super) struct TraceChunkPath {
    id: String,
    chunk: usize,
}

pub(super) async fn list_traces(
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

pub(super) async fn get_trace(
    Extension(resources): Extension<Arc<TenantResources>>,
    AxumPath(id): AxumPath<String>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let trace = resources.store.get(&id).await.map_err(ApiError::internal)?;
    match trace {
        Some(value) => Ok(Json(json!({ "trace": value }))),
        None => Err(ApiError::not_found("trace not found")),
    }
}

pub(super) async fn get_trace_manifest(
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

pub(super) async fn get_trace_records_chunk(
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

pub(super) async fn get_trace_nodes_chunk(
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

pub(super) async fn get_trace_finalize(
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

pub(super) async fn stream_traces(
    Extension(resources): Extension<Arc<TenantResources>>,
) -> Sse<impl tokio_stream::Stream<Item = std::result::Result<Event, Infallible>>> {
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

pub(super) async fn get_api_graph(
    Extension(resources): Extension<Arc<TenantResources>>,
) -> std::result::Result<Json<ApiGraphResponse>, ApiError> {
    let graph = build_api_graph(&resources.rules_dir).map_err(ApiError::internal)?;
    Ok(Json(graph))
}
