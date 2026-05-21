#[tokio::test]
async fn request_body_too_large_writes_trace() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
    );

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true)
            .with_max_body_bytes(16),
    )
    .expect("load engine");

    let body = vec![b'a'; 64];
    let request = Request::builder()
        .method("POST")
        .uri("/api/test")
        .body(axum::body::Body::from(body))
        .expect("build request");

    let err = engine
        .handle_request(request)
        .await
        .expect_err("handle request should fail");
    assert!(format!("{err}").contains("payload too large"));

    let items = wait_for_trace_items(rules_dir).await;
    assert!(!items.is_empty());
    assert!(items.iter().any(|item| item.status == "error"));
}
