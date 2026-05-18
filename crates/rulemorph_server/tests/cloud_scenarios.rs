use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use http_body_util::BodyExt;
use rulemorph_endpoint::{EndpointEngine, EngineConfig};
use rulemorph_server::{
    ApiMode, AppState, RateLimiter, TenantContext, TenantRegistry, TenantResolver, TenantResources,
    build_router,
};
use rulemorph_trace::{ImportResult, TraceStore};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::sync::broadcast;
use tower::Service;
use tower::ServiceExt;
use zip::{CompressionMethod, write::FileOptions};

include!("cloud_scenarios/trace_detail.rs");

async fn request_json(app: &Router, path: String) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
        .await
        .expect("request");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    let value = if body.is_empty() {
        json!(null)
    } else {
        serde_json::from_slice(&body).expect("json")
    };
    (status, value)
}

async fn request_json_with_headers(
    app: &Router,
    path: String,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(&path);
    for (key, value) in headers {
        builder = builder.header(*key, *value);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .expect("request");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    let value = if body.is_empty() {
        json!(null)
    } else {
        serde_json::from_slice(&body).expect("json")
    };
    (status, value)
}

async fn request_json_post_with_headers(
    app: &Router,
    path: String,
    headers: &[(&str, &str)],
    body: Value,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(&path)
        .header("content-type", "application/json");
    for (key, value) in headers {
        builder = builder.header(*key, *value);
    }
    let payload = serde_json::to_vec(&body).expect("serialize request body");
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(payload)).unwrap())
        .await
        .expect("request");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    let value = if body.is_empty() {
        json!(null)
    } else {
        serde_json::from_slice(&body).expect("json")
    };
    (status, value)
}

async fn post_api_import(
    app: &Router,
    label: &str,
    authorization: Option<&str>,
    import_kind: Option<&str>,
    boundary: &str,
    body: Vec<u8>,
) -> Response {
    let mut builder = Request::builder().method("POST").uri("/api/import").header(
        "content-type",
        format!("multipart/form-data; boundary={boundary}"),
    );
    if let Some(authorization) = authorization {
        builder = builder.header("authorization", authorization);
    }
    if let Some(import_kind) = import_kind {
        builder = builder.header("x-rulemorph-import", import_kind);
    }

    app.clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap_or_else(|_| panic!("{label}"))
}

async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
    let payload = response.into_body().collect().await?.to_bytes();
    Ok(serde_json::from_slice(&payload)?)
}

async fn wait_for_trace_id(app: &Router) -> String {
    for _ in 0..40 {
        let (status, list) = request_json(app, "/internal/traces".to_string()).await;
        if status == StatusCode::OK {
            if let Some(trace_id) = list
                .get("traces")
                .and_then(|value| value.as_array())
                .and_then(|values| values.first())
                .and_then(|value| value.get("trace_id"))
                .and_then(|value| value.as_str())
            {
                return trace_id.to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("trace not found after waiting");
}

async fn default_test_resources(
    data_dir: PathBuf,
    rules_dir: PathBuf,
) -> Result<Arc<TenantResources>> {
    let store = TraceStore::new(data_dir.clone()).await?;
    let (trace_events, _) = broadcast::channel(16);
    let auth_dir = data_dir.join("auth");
    fs::create_dir_all(&auth_dir)?;
    Ok(Arc::new(TenantResources {
        tenant_id: "default".to_string(),
        data_dir,
        rules_dir,
        auth_dir,
        store: Arc::new(store),
        api_engine: None,
        trace_events,
    }))
}

struct StaticTenantResolver {
    api_key: String,
    tenant_id: String,
}

#[async_trait]
impl TenantResolver for StaticTenantResolver {
    async fn resolve(&self, api_key: &str) -> Result<Option<TenantContext>> {
        if api_key == self.api_key {
            Ok(Some(TenantContext::new(self.tenant_id.clone())))
        } else {
            Ok(None)
        }
    }
}

struct CountingTenantResolver {
    api_key: String,
    tenant_id: String,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl TenantResolver for CountingTenantResolver {
    async fn resolve(&self, api_key: &str) -> Result<Option<TenantContext>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if api_key == self.api_key {
            Ok(Some(TenantContext::new(self.tenant_id.clone())))
        } else {
            Ok(None)
        }
    }
}

struct RejectTenantResolver;

#[async_trait]
impl TenantResolver for RejectTenantResolver {
    async fn resolve(&self, _api_key: &str) -> Result<Option<TenantContext>> {
        Ok(None)
    }
}

async fn build_v1_app(
    api_key_resolver: Option<Arc<dyn TenantResolver>>,
    rate_limit_per_sec: Option<u64>,
) -> (Router, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let rules_dir = temp.path().join("rules");
    let data_dir = temp.path().join("data");
    fs::create_dir_all(rules_dir.join("rules")).expect("create rules");
    fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /v1/test
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
      body:
        ok: true
  - method: GET
    path: /api/test
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
      body:
        ok: true
"#,
    )
    .expect("write endpoint.yaml");
    fs::write(
        rules_dir.join("rules/ok.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "output.value"
    value: 1
finalize:
  wrap:
    key: result
"#,
    )
    .expect("write ok.yaml");

    let engine = EndpointEngine::load(
        rules_dir.clone(),
        EngineConfig::new("http://127.0.0.1:8080".to_string(), data_dir.clone()),
    )
    .expect("load engine");
    let store = TraceStore::new(data_dir.clone())
        .await
        .expect("trace store");
    let (trace_events, _) = broadcast::channel(16);
    let auth_dir = data_dir.join("auth");
    fs::create_dir_all(&auth_dir).expect("create auth dir");
    let resources = TenantResources {
        tenant_id: "default".to_string(),
        data_dir: data_dir.clone(),
        rules_dir: rules_dir.clone(),
        auth_dir,
        store: Arc::new(store),
        api_engine: Some(Arc::new(engine)),
        trace_events,
    };
    let state = AppState {
        default_resources: Arc::new(resources),
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::Rules,
        tenant_resolver: api_key_resolver,
        internal_api_key: None,
        allow_unauth_internal: false,
        rate_limiter: rate_limit_per_sec.map(RateLimiter::new).map(Arc::new),
    };
    (build_router(state, false), temp)
}

include!("cloud_scenarios/v1_auth.rs");
include!("cloud_scenarios/internal_auth.rs");
include!("cloud_scenarios/tenant_traces.rs");

fn build_zip_import_payload(trace_id: &str) -> Result<(String, Vec<u8>)> {
    let trace = json!({
        "trace_schema_version": 1,
        "trace_id": trace_id,
        "timestamp": "2026-02-03T00:00:00Z",
        "status": "ok",
        "summary": { "record_total": 1, "record_success": 1, "record_failed": 0 }
    });
    let trace_payload = serde_json::to_vec(&trace)?;

    let mut zip_writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = FileOptions::default().compression_method(CompressionMethod::Stored);
    zip_writer.start_file(format!("traces/2026/02/03/{trace_id}/trace.json"), options)?;
    zip_writer.write_all(&trace_payload)?;
    let zip_cursor = zip_writer.finish()?;
    let zip_bytes = zip_cursor.into_inner();

    let boundary = "BOUNDARY".to_string();
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"bundle\"; filename=\"bundle.zip\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: application/zip\r\n\r\n");
    body.extend_from_slice(&zip_bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    Ok((boundary, body))
}

include!("cloud_scenarios/api_import.rs");
