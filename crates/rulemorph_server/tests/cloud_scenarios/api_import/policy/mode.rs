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
