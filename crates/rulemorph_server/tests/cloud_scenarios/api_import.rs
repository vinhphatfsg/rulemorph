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

    let response = post_api_import(
        &app,
        "api import response",
        Some("Bearer internal-key"),
        Some("zip"),
        &boundary,
        body,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let result = read_json::<ImportResult>(response).await?;
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
    let response = post_api_import(
        &app,
        "shadow unauth response",
        None,
        Some("zip"),
        &boundary,
        zip_body,
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let (boundary, zip_body) = build_zip_import_payload("zip-shadow-001")?;
    let response = post_api_import(
        &app,
        "shadow import response",
        Some("Bearer internal-key"),
        Some("zip"),
        &boundary,
        zip_body,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = read_json::<Value>(response).await?;
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
    let response = post_api_import(
        &app,
        "tenant import response",
        Some("Bearer tenant-key"),
        Some("zip"),
        &boundary,
        body,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = read_json::<Value>(response).await?;
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

    let response = post_api_import(
        &app,
        "invalid api key response",
        Some("Bearer invalid"),
        None,
        "BOUNDARY",
        Vec::new(),
    )
    .await;
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

    let missing = post_api_import(
        &app,
        "missing auth response",
        None,
        None,
        &boundary,
        body.clone(),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let invalid = post_api_import(
        &app,
        "invalid auth response",
        Some("Bearer invalid"),
        None,
        &boundary,
        body,
    )
    .await;
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

    let response = post_api_import(
        &app,
        "tenant id response",
        Some("Bearer internal-key"),
        Some("zip"),
        &boundary,
        body,
    )
    .await;
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

    let first = post_api_import(
        &app,
        "first response",
        Some("Bearer internal-key"),
        None,
        &boundary,
        body.clone(),
    )
    .await;
    assert_eq!(first.status(), StatusCode::BAD_REQUEST);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let second = post_api_import(
        &app,
        "second response",
        Some("Bearer internal-key"),
        None,
        &boundary,
        body,
    )
    .await;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

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

    let response = post_api_import(
        &app,
        "no ui import response",
        None,
        Some("zip"),
        &boundary,
        body,
    )
    .await;
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

    let response = post_api_import(
        &app,
        "no ui auth response",
        Some("Bearer internal-key"),
        Some("zip"),
        &boundary,
        body,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let result = read_json::<ImportResult>(response).await?;
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

    let response = post_api_import(
        &app,
        "ui only import response",
        None,
        Some("zip"),
        &boundary,
        body,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let result = read_json::<ImportResult>(response).await?;
    assert_eq!(result.imported, 1);

    Ok(())
}
