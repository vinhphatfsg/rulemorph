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

async fn wait_for_tenant_trace_list(app: &Router, tenant_id: &str) -> Value {
    for _ in 0..40 {
        let (status, list) = request_json_with_headers(
            app,
            "/internal/traces".to_string(),
            &[
                ("authorization", "Bearer internal-key"),
                ("x-tenant-id", tenant_id),
            ],
        )
        .await;
        if status == StatusCode::OK {
            if list
                .get("traces")
                .and_then(|value| value.as_array())
                .is_some_and(|values| !values.is_empty())
            {
                return list;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("tenant trace not found after waiting: {tenant_id}");
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

struct MapTenantResolver {
    map: HashMap<String, String>,
}

#[async_trait]
impl TenantResolver for MapTenantResolver {
    async fn resolve(&self, api_key: &str) -> Result<Option<TenantContext>> {
        Ok(self
            .map
            .get(api_key)
            .map(|tenant_id| TenantContext::new(tenant_id.clone())))
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

#[tokio::test]
async fn api_import_rate_limit_uses_internal_bucket_without_tenant() -> Result<()> {
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
    )?;
    let store = TraceStore::new(data_dir.clone()).await?;
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
        tenant_resolver: None,
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: Some(Arc::new(RateLimiter::new(1))),
    };
    let app = build_router(state, true);

    let api_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("api response");
    assert_eq!(api_response.status(), StatusCode::OK);

    let (boundary, body) = build_zip_import_payload("zip-rate-bucket-001")?;
    let import_response = post_api_import(
        &app,
        "import response",
        Some("Bearer internal-key"),
        Some("zip"),
        &boundary,
        body,
    )
    .await;
    assert_eq!(import_response.status(), StatusCode::OK);
    let result = read_json::<ImportResult>(import_response).await?;
    assert_eq!(result.imported, 1);

    Ok(())
}

#[tokio::test]
async fn internal_requires_key_when_configured() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources = default_test_resources(data_dir.clone(), data_dir.join("api_rules"))
        .await
        .expect("default resources");
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::UiOnly,
        tenant_resolver: None,
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/traces")
                .header("x-tenant-id", "tenant-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/traces")
                .header("authorization", "Bearer internal-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn internal_requires_key_when_tenant_resolver_set() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let resolver = Arc::new(StaticTenantResolver {
        api_key: "valid-key".to_string(),
        tenant_id: "tenant-1".to_string(),
    });
    let default_resources = default_test_resources(data_dir.clone(), data_dir.join("api_rules"))
        .await
        .expect("default resources");
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::UiOnly,
        tenant_resolver: Some(resolver),
        internal_api_key: None,
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/traces")
                .header("x-tenant-id", "tenant-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn internal_unauthorized_request_does_not_initialize_tenant() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let resolver = Arc::new(StaticTenantResolver {
        api_key: "valid-key".to_string(),
        tenant_id: "tenant-1".to_string(),
    });
    let registry = Arc::new(TenantRegistry::new(
        data_dir.clone(),
        None,
        ApiMode::UiOnly,
        true,
        8080,
        Vec::new(),
        true,
        Some("internal-key".to_string()),
    ));
    let default_resources = registry
        .get_or_init("default")
        .await
        .expect("default resources");
    let state = AppState {
        default_resources,
        tenant_registry: Some(registry),
        ui_source: None,
        api_mode: ApiMode::UiOnly,
        tenant_resolver: Some(resolver),
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/traces")
                .header("x-tenant-id", "tenant-attack")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(!data_dir.join("tenants/tenant-attack").exists());
}

#[tokio::test]
async fn internal_requires_tenant_id_when_resolver_set() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources = default_test_resources(data_dir.clone(), data_dir.join("api_rules"))
        .await
        .expect("default resources");
    let resolver = Arc::new(StaticTenantResolver {
        api_key: "valid-key".to_string(),
        tenant_id: "tenant-1".to_string(),
    });
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::UiOnly,
        tenant_resolver: Some(resolver),
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/traces")
                .header("authorization", "Bearer internal-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn internal_api_key_issue_is_serialized_per_store() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources = default_test_resources(data_dir.clone(), data_dir.join("api_rules"))
        .await
        .expect("default resources");
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::UiOnly,
        tenant_resolver: None,
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);
    let issue_count = 32usize;

    let mut handles = Vec::with_capacity(issue_count);
    for i in 0..issue_count {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            let (status, payload) = request_json_post_with_headers(
                &app,
                "/internal/api-keys".to_string(),
                &[("authorization", "Bearer internal-key")],
                json!({ "label": format!("parallel-{i}") }),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            payload
                .get("id")
                .and_then(|value| value.as_str())
                .expect("issued id")
                .to_string()
        }));
    }

    let mut ids = Vec::with_capacity(issue_count);
    for handle in handles {
        ids.push(handle.await.expect("join"));
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), issue_count);

    let (status, list) = request_json_with_headers(
        &app,
        "/internal/api-keys".to_string(),
        &[("authorization", "Bearer internal-key")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let keys = list
        .get("keys")
        .and_then(|value| value.as_array())
        .expect("keys");
    assert_eq!(keys.len(), issue_count);
}

#[tokio::test]
async fn tenant_traces_are_isolated() {
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

    let resolver = Arc::new(MapTenantResolver {
        map: HashMap::from([
            ("key-a".to_string(), "tenant-a".to_string()),
            ("key-b".to_string(), "tenant-b".to_string()),
        ]),
    });

    let registry = Arc::new(TenantRegistry::new(
        data_dir.clone(),
        Some(rules_dir.clone()),
        ApiMode::Rules,
        true,
        8080,
        Vec::new(),
        true,
        Some("internal-key".to_string()),
    ));
    let default_resources = registry
        .get_or_init("default")
        .await
        .expect("default resources");
    let state = AppState {
        default_resources,
        tenant_registry: Some(registry),
        ui_source: None,
        api_mode: ApiMode::Rules,
        tenant_resolver: Some(resolver),
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/test")
                .header("authorization", "Bearer key-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/test")
                .header("authorization", "Bearer key-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let (status_a, _list_a) = request_json_with_headers(
        &app,
        "/internal/traces".to_string(),
        &[("x-tenant-id", "tenant-a")],
    )
    .await;
    assert_eq!(status_a, StatusCode::UNAUTHORIZED);

    let list_a = wait_for_tenant_trace_list(&app, "tenant-a").await;
    let count_a = list_a
        .get("traces")
        .and_then(|value| value.as_array())
        .map(|values| values.len())
        .unwrap_or(0);
    assert_eq!(count_a, 1);

    let list_b = wait_for_tenant_trace_list(&app, "tenant-b").await;
    let count_b = list_b
        .get("traces")
        .and_then(|value| value.as_array())
        .map(|values| values.len())
        .unwrap_or(0);
    assert_eq!(count_b, 1);
    assert_ne!(
        list_a
            .get("traces")
            .and_then(|value| value.as_array())
            .and_then(|values| values.first())
            .and_then(|value| value.get("trace_id"))
            .and_then(|value| value.as_str()),
        list_b
            .get("traces")
            .and_then(|value| value.as_array())
            .and_then(|values| values.first())
            .and_then(|value| value.get("trace_id"))
            .and_then(|value| value.as_str()),
    );
}

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

#[tokio::test]
async fn internal_import_zip_route_is_removed() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources =
        default_test_resources(data_dir.clone(), data_dir.join("api_rules")).await?;
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::UiOnly,
        tenant_resolver: None,
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/import-zip")
                .header("authorization", "Bearer internal-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn internal_import_path_requires_auth_before_bundle_validation() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources =
        default_test_resources(data_dir.clone(), data_dir.join("api_rules")).await?;
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::UiOnly,
        tenant_resolver: None,
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/import")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "bundle_path": "/definitely/not/a/rulemorph/bundle" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    Ok(())
}

#[tokio::test]
async fn internal_import_path_rejects_non_temp_bundle_path_after_auth() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources =
        default_test_resources(data_dir.clone(), data_dir.join("api_rules")).await?;
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::UiOnly,
        tenant_resolver: None,
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);
    let temp_root = std::env::temp_dir()
        .canonicalize()
        .unwrap_or_else(|_| std::env::temp_dir());
    let non_temp_dir = temp_root
        .parent()
        .expect("temp dir has a parent")
        .to_path_buf();
    assert!(
        !non_temp_dir.starts_with(&temp_root),
        "test fixture path must be outside temp dir"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/import")
                .header("authorization", "Bearer internal-key")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "bundle_path": non_temp_dir }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}
