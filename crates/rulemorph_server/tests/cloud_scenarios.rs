use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Write};
use std::sync::Arc;
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
async fn v1_returns_503_when_resolver_missing() {
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
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
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
async fn internal_requires_key_when_configured() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
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
    let store = TraceStore::new(data_dir.clone())
        .await
        .expect("trace store");
    let (trace_events, _) = broadcast::channel(16);
    let resolver = Arc::new(StaticTenantResolver {
        api_key: "valid-key".to_string(),
        tenant_id: "tenant-1".to_string(),
    });
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
async fn internal_requires_tenant_id_when_resolver_set() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
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
    let resolver = Arc::new(StaticTenantResolver {
        api_key: "valid-key".to_string(),
        tenant_id: "tenant-1".to_string(),
    });
    let state = AppState {
        default_resources: Arc::new(resources),
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

    let (status_a, list_a) = request_json_with_headers(
        &app,
        "/internal/traces".to_string(),
        &[
            ("authorization", "Bearer internal-key"),
            ("x-tenant-id", "tenant-a"),
        ],
    )
    .await;
    assert_eq!(status_a, StatusCode::OK);
    let count_a = list_a
        .get("traces")
        .and_then(|value| value.as_array())
        .map(|values| values.len())
        .unwrap_or(0);
    assert_eq!(count_a, 1);

    let (status_b, list_b) = request_json_with_headers(
        &app,
        "/internal/traces".to_string(),
        &[
            ("authorization", "Bearer internal-key"),
            ("x-tenant-id", "tenant-b"),
        ],
    )
    .await;
    assert_eq!(status_b, StatusCode::OK);
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

#[tokio::test]
async fn import_zip_bundle_adds_traces() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let store = TraceStore::new(data_dir.clone()).await?;
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

    let trace = json!({
        "trace_schema_version": 1,
        "trace_id": "zip-001",
        "timestamp": "2026-02-03T00:00:00Z",
        "status": "ok",
        "summary": { "record_total": 1, "record_success": 1, "record_failed": 0 }
    });
    let trace_payload = serde_json::to_vec(&trace)?;

    let mut zip_writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = FileOptions::default().compression_method(CompressionMethod::Stored);
    zip_writer.start_file("traces/2026/02/03/zip-001/trace.json", options)?;
    zip_writer.write_all(&trace_payload)?;
    let zip_cursor = zip_writer.finish()?;
    let zip_bytes = zip_cursor.into_inner();

    let boundary = "BOUNDARY";
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"bundle\"; filename=\"bundle.zip\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: application/zip\r\n\r\n");
    body.extend_from_slice(&zip_bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/import-zip")
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

    let (status, list) = request_json(&app, "/internal/traces".to_string()).await;
    assert_eq!(status, StatusCode::OK);
    let count = list
        .get("traces")
        .and_then(|value| value.as_array())
        .map(|values| values.len())
        .unwrap_or(0);
    assert_eq!(count, 1);

    Ok(())
}
