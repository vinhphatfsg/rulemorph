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
