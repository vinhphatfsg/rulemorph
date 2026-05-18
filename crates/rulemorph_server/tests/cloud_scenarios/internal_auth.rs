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
