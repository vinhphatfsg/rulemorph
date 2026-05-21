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
