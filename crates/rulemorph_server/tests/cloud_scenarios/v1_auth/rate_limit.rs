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
