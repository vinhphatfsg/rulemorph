#[tokio::test]
async fn v1_is_not_mounted_when_resolver_missing() {
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
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn tenant_api_rules_allow_internal_auth() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let rules_dir = data_dir.join("tenants").join("default").join("api_rules");
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

    let registry = TenantRegistry::new(
        data_dir,
        None,
        ApiMode::Rules,
        true,
        8080,
        Vec::new(),
        true,
        Some("internal-key".to_string()),
    );
    let resources = registry.get_or_init("default").await?;
    let engine = resources.api_engine.as_ref().expect("api engine");

    assert!(engine.allows_internal_auth());
    Ok(())
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
