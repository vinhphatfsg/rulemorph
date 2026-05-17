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
use http_body_util::BodyExt;
use rulemorph_endpoint::{EndpointEngine, EngineConfig};
use rulemorph_server::{
    ApiMode, AppState, RateLimiter, TenantContext, TenantRegistry, TenantResolver, TenantResources,
    build_router,
};
use rulemorph_trace::{ImportResult, TraceStore};
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::sync::broadcast;
use tower::Service;
use tower::ServiceExt;
use zip::{CompressionMethod, write::FileOptions};

#[tokio::test]
async fn cloud_scenario_trace_full_detail_roundtrip() {
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
        tenant_resolver: None,
        internal_api_key: None,
        allow_unauth_internal: true,
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("api response");
    assert_eq!(response.status(), StatusCode::OK);

    let trace_id = wait_for_trace_id(&app).await;

    let (status, manifest) =
        request_json(&app, format!("/internal/traces/{}/manifest", trace_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        manifest
            .get("manifest")
            .and_then(|value| value.get("detail"))
            .and_then(|value| value.get("status"))
            .and_then(|value| value.as_str()),
        Some("full")
    );

    let (status, records) =
        request_json(&app, format!("/internal/traces/{}/records/0", trace_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        records
            .get("records")
            .and_then(|value| value.as_array())
            .is_some_and(|values| !values.is_empty())
    );

    let (status, nodes) =
        request_json(&app, format!("/internal/traces/{}/nodes/0", trace_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        nodes
            .get("nodes")
            .and_then(|value| value.as_array())
            .is_some_and(|values| !values.is_empty())
    );

    let finalize_ref = manifest
        .get("manifest")
        .and_then(|value| value.get("detail"))
        .and_then(|value| value.get("finalize"))
        .filter(|value| !value.is_null());
    let (status, finalize) =
        request_json(&app, format!("/internal/traces/{}/finalize", trace_id)).await;
    if finalize_ref.is_some() {
        assert_eq!(status, StatusCode::OK);
        assert!(finalize.get("finalize").is_some());
    } else {
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn cloud_scenario_basic_detail_fallback() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let trace_dir = data_dir.join("traces/2026/02/03/basic-001");
    fs::create_dir_all(&trace_dir).expect("create trace dir");

    let trace = json!({
        "trace_schema_version": 1,
        "trace_id": "basic-001",
        "timestamp": "2026-02-03T00:00:00Z",
        "status": "ok",
        "rule": { "type": "normal", "name": "basic", "path": "rules/basic.yaml", "version": 2 },
        "detail": {
            "layout": "records_nodes_split",
            "status": "basic",
            "reason": ["budget_exceeded"],
            "records": [],
            "nodes": []
        }
    });
    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_string_pretty(&trace).expect("serialize trace"),
    )
    .expect("write trace.json");

    let store = TraceStore::new(data_dir.clone())
        .await
        .expect("trace store");
    let (trace_events, _) = broadcast::channel(16);
    let auth_dir = data_dir.join("auth");
    fs::create_dir_all(&auth_dir).expect("create auth dir");
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
        tenant_resolver: None,
        internal_api_key: None,
        allow_unauth_internal: true,
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let (status, list) = request_json(&app, "/internal/traces".to_string()).await;
    assert_eq!(status, StatusCode::OK);
    let trace_id = list
        .get("traces")
        .and_then(|value| value.as_array())
        .and_then(|values| values.first())
        .and_then(|value| value.get("trace_id"))
        .and_then(|value| value.as_str())
        .expect("trace id")
        .to_string();

    let (status, manifest) =
        request_json(&app, format!("/internal/traces/{}/manifest", trace_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        manifest
            .get("manifest")
            .and_then(|value| value.get("detail"))
            .and_then(|value| value.get("status"))
            .and_then(|value| value.as_str()),
        Some("basic")
    );

    let (status, _) = request_json(&app, format!("/internal/traces/{}/records/0", trace_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = request_json(&app, format!("/internal/traces/{}/nodes/0", trace_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = request_json(&app, format!("/internal/traces/{}/finalize", trace_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

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

#[tokio::test]
async fn v1_is_not_mounted_when_resolver_missing() {
    let (app, _temp) = build_v1_app(None, None).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn tenant_api_rules_allow_internal_auth() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let rules_dir = data_dir.join("tenants").join("default").join("api_rules");
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

    let registry = TenantRegistry::new(
        data_dir,
        None,
        ApiMode::Rules,
        true,
        8080,
        Vec::new(),
        true,
        Some("internal-key".to_string()),
    );
    let resources = registry.get_or_init("default").await?;
    let engine = resources.api_engine.as_ref().expect("api engine");

    assert!(engine.allows_internal_auth());
    Ok(())
}

#[tokio::test]
async fn v1_returns_401_for_invalid_key() {
    let resolver = Arc::new(RejectTenantResolver);
    let (app, _temp) = build_v1_app(Some(resolver), None).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/test")
                .header("authorization", "Bearer invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn v1_invalid_key_is_rate_limited() {
    let resolver = Arc::new(RejectTenantResolver);
    let (mut app, _temp) = build_v1_app(Some(resolver), Some(1)).await;

    let response = <Router as ServiceExt<Request<Body>>>::ready(&mut app)
        .await
        .expect("ready")
        .call(
            Request::builder()
                .uri("/v1/test")
                .header("authorization", "Bearer invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = <Router as ServiceExt<Request<Body>>>::ready(&mut app)
        .await
        .expect("ready")
        .call(
            Request::builder()
                .uri("/v1/test")
                .header("authorization", "Bearer invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn v1_returns_200_for_valid_key() {
    let resolver = Arc::new(StaticTenantResolver {
        api_key: "valid-key".to_string(),
        tenant_id: "tenant-1".to_string(),
    });
    let (app, _temp) = build_v1_app(Some(resolver), None).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/test")
                .header("authorization", "Bearer valid-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn api_requires_auth_when_resolver_set() {
    let resolver = Arc::new(StaticTenantResolver {
        api_key: "valid-key".to_string(),
        tenant_id: "tenant-1".to_string(),
    });
    let (app, _temp) = build_v1_app(Some(resolver), None).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn v1_rate_limit_returns_429() {
    let resolver = Arc::new(StaticTenantResolver {
        api_key: "valid-key".to_string(),
        tenant_id: "tenant-1".to_string(),
    });
    let (mut app, _temp) = build_v1_app(Some(resolver), Some(1)).await;

    let response = <Router as ServiceExt<Request<Body>>>::ready(&mut app)
        .await
        .expect("ready")
        .call(
            Request::builder()
                .uri("/v1/test")
                .header("authorization", "Bearer valid-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = <Router as ServiceExt<Request<Body>>>::ready(&mut app)
        .await
        .expect("ready")
        .call(
            Request::builder()
                .uri("/v1/test")
                .header("authorization", "Bearer valid-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn api_rate_limit_applies_without_resolver() {
    let (mut app, _temp) = build_v1_app(None, Some(1)).await;

    let response = <Router as ServiceExt<Request<Body>>>::ready(&mut app)
        .await
        .expect("ready")
        .call(
            Request::builder()
                .uri("/api/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = <Router as ServiceExt<Request<Body>>>::ready(&mut app)
        .await
        .expect("ready")
        .call(
            Request::builder()
                .uri("/api/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

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
    let import_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("authorization", "Bearer internal-key")
                .header("x-rulemorph-import", "zip")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("import response");
    assert_eq!(import_response.status(), StatusCode::OK);
    let payload = import_response.into_body().collect().await?.to_bytes();
    let result: ImportResult = serde_json::from_slice(&payload)?;
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

#[tokio::test]
async fn api_import_zip_bundle_adds_traces() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources =
        default_test_resources(data_dir.clone(), data_dir.join("api_rules")).await?;
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::Rules,
        tenant_resolver: None,
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);
    let (boundary, body) = build_zip_import_payload("zip-001")?;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("authorization", "Bearer internal-key")
                .header("x-rulemorph-import", "zip")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let payload = response.into_body().collect().await?.to_bytes();
    let result: ImportResult = serde_json::from_slice(&payload)?;
    assert_eq!(result.imported, 1);

    let (status, list) = request_json_with_headers(
        &app,
        "/internal/traces".to_string(),
        &[("authorization", "Bearer internal-key")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let count = list
        .get("traces")
        .and_then(|value| value.as_array())
        .map(|values| values.len())
        .unwrap_or(0);
    assert_eq!(count, 1);

    Ok(())
}

#[tokio::test]
async fn api_import_does_not_shadow_rule_endpoint() -> Result<()> {
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
  - method: POST
    path: /api/import
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
      body:
        kind: rule
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
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let (status, body) =
        request_json_post_with_headers(&app, "/api/import".to_string(), &[], json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.get("kind").and_then(|value| value.as_str()),
        Some("rule")
    );

    let (boundary, zip_body) = build_zip_import_payload("zip-shadow-unauth")?;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("x-rulemorph-import", "zip")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(zip_body))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let (boundary, zip_body) = build_zip_import_payload("zip-shadow-001")?;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("authorization", "Bearer internal-key")
                .header("x-rulemorph-import", "zip")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(zip_body))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = response.into_body().collect().await?.to_bytes();
    let body: Value = serde_json::from_slice(&payload)?;
    assert_eq!(
        body.get("kind").and_then(|value| value.as_str()),
        Some("rule")
    );

    Ok(())
}

#[tokio::test]
async fn api_import_dispatch_uses_authenticated_tenant_rules() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_rules_dir = data_dir.join("tenants/default/api_rules");
    let tenant_rules_dir = data_dir.join("tenants/tenant-a/api_rules");
    fs::create_dir_all(default_rules_dir.join("rules")).expect("create default rules");
    fs::create_dir_all(tenant_rules_dir.join("rules")).expect("create tenant rules");

    fs::write(
        default_rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/default
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
      body:
        kind: default
"#,
    )
    .expect("write default endpoint.yaml");
    fs::write(
        default_rules_dir.join("rules/ok.yaml"),
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
    .expect("write default ok.yaml");

    fs::write(
        tenant_rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/import
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
      body:
        kind: tenant-rule
"#,
    )
    .expect("write tenant endpoint.yaml");
    fs::write(
        tenant_rules_dir.join("rules/ok.yaml"),
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
    .expect("write tenant ok.yaml");

    let resolver = Arc::new(StaticTenantResolver {
        api_key: "tenant-key".to_string(),
        tenant_id: "tenant-a".to_string(),
    });
    let registry = Arc::new(TenantRegistry::new(
        data_dir.clone(),
        None,
        ApiMode::Rules,
        true,
        8080,
        Vec::new(),
        true,
        Some("internal-key".to_string()),
    ));
    let default_resources = registry.get_or_init("default").await?;
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

    let (boundary, body) = build_zip_import_payload("zip-tenant-rule-001")?;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("authorization", "Bearer tenant-key")
                .header("x-rulemorph-import", "zip")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = response.into_body().collect().await?.to_bytes();
    let body: Value = serde_json::from_slice(&payload)?;
    assert_eq!(
        body.get("kind").and_then(|value| value.as_str()),
        Some("tenant-rule")
    );

    Ok(())
}

#[tokio::test]
async fn api_import_dispatch_invalid_api_key_resolves_tenant_once() -> Result<()> {
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
  - method: POST
    path: /api/import
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
      body:
        kind: rule
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
    let calls = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(CountingTenantResolver {
        api_key: "tenant-key".to_string(),
        tenant_id: "tenant-a".to_string(),
        calls: calls.clone(),
    });
    let state = AppState {
        default_resources: Arc::new(resources),
        tenant_registry: None,
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
                .method("POST")
                .uri("/api/import")
                .header("authorization", "Bearer invalid")
                .header("content-type", "multipart/form-data; boundary=BOUNDARY")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    Ok(())
}

#[tokio::test]
async fn api_import_requires_internal_key() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources =
        default_test_resources(data_dir.clone(), data_dir.join("api_rules")).await?;
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::Rules,
        tenant_resolver: None,
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);
    let (boundary, body) = build_zip_import_payload("zip-auth-001")?;

    let missing = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .expect("missing auth response");
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let invalid = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("authorization", "Bearer invalid")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("invalid auth response");
    assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);

    Ok(())
}

#[tokio::test]
async fn api_import_requires_tenant_id_when_resolver_set() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources =
        default_test_resources(data_dir.clone(), data_dir.join("api_rules")).await?;
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::Rules,
        tenant_resolver: Some(Arc::new(RejectTenantResolver)),
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);
    let (boundary, body) = build_zip_import_payload("zip-tenant-001")?;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("authorization", "Bearer internal-key")
                .header("x-rulemorph-import", "zip")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test]
async fn api_import_dispatch_rate_limits_before_tenant_resolver() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources =
        default_test_resources(data_dir.clone(), data_dir.join("api_rules")).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(CountingTenantResolver {
        api_key: "internal-key".to_string(),
        tenant_id: "tenant-a".to_string(),
        calls: calls.clone(),
    });
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::Rules,
        tenant_resolver: Some(resolver),
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: Some(Arc::new(RateLimiter::new(1))),
    };
    let app = build_router(state, true);
    let (boundary, body) = build_zip_import_payload("zip-rate-limit-001")?;

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("authorization", "Bearer internal-key")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .expect("first response");
    assert_eq!(first.status(), StatusCode::BAD_REQUEST);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let second = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("authorization", "Bearer internal-key")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("second response");
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    Ok(())
}

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

#[tokio::test]
async fn api_import_requires_internal_key_without_ui() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources =
        default_test_resources(data_dir.clone(), data_dir.join("api_rules")).await?;
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::Rules,
        tenant_resolver: None,
        internal_api_key: None,
        allow_unauth_internal: true,
        rate_limiter: None,
    };
    let app = build_router(state, false);
    let (boundary, body) = build_zip_import_payload("zip-no-ui-001")?;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("x-rulemorph-import", "zip")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    Ok(())
}

#[tokio::test]
async fn api_import_route_works_without_ui_when_internal_key_provided() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources =
        default_test_resources(data_dir.clone(), data_dir.join("api_rules")).await?;
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::Rules,
        tenant_resolver: None,
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: true,
        rate_limiter: None,
    };
    let app = build_router(state, false);
    let (boundary, body) = build_zip_import_payload("zip-no-ui-auth-001")?;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("authorization", "Bearer internal-key")
                .header("x-rulemorph-import", "zip")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = response.into_body().collect().await?.to_bytes();
    let result: ImportResult = serde_json::from_slice(&payload)?;
    assert_eq!(result.imported, 1);

    Ok(())
}

#[tokio::test]
async fn api_import_route_works_in_ui_only_mode() -> Result<()> {
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
        internal_api_key: None,
        allow_unauth_internal: true,
        rate_limiter: None,
    };
    let app = build_router(state, true);
    let (boundary, body) = build_zip_import_payload("zip-ui-only-001")?;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import")
                .header("x-rulemorph-import", "zip")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = response.into_body().collect().await?.to_bytes();
    let result: ImportResult = serde_json::from_slice(&payload)?;
    assert_eq!(result.imported, 1);

    Ok(())
}
