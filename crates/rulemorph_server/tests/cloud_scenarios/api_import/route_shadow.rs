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
