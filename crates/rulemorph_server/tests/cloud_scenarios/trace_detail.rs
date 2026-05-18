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
