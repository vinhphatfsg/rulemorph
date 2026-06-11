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
    let registry = Arc::new(TenantRegistry::new(TenantRegistryConfig {
        base_dir: data_dir.clone(),
        rules_dir: None,
        api_mode: ApiMode::UiOnly,
        ui_enabled: true,
        port: 8080,
        ssrf_allowlist: Vec::new(),
        ssrf_allow_private: true,
        internal_api_key: Some("internal-key".to_string()),
    }));
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
