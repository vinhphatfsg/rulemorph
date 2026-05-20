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
