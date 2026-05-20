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
