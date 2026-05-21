#[tokio::test]
async fn network_response_too_large_returns_error() {
    let payload = "x".repeat(2048);
    let app = axum::Router::new().route(
        "/data",
        axum::routing::get({
            let payload = payload.clone();
            move || {
                let payload = payload.clone();
                async move { axum::Json(json!({ "data": payload })) }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let host = format!("localhost:{}", addr.port());
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, app.into_make_service()).with_graceful_shutdown(async {
        let _ = shutdown_rx.await;
    });
    let server_handle = tokio::spawn(async move {
        let _ = server.await;
    });

    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = rules_dir.join("rules");
    std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
    )
    .expect("write endpoint.yaml");

    std::fs::write(
        rules_subdir.join("network.yaml"),
        format!(
            r#"
version: 2
type: network
request:
  method: GET
  url: "http://{}/data"
timeout: 1s
"#,
            host
        ),
    )
    .expect("write network.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true)
            .with_max_response_bytes(128),
    )
    .expect("load engine");

    let request = Request::builder()
        .method("GET")
        .uri("/api/test")
        .body(axum::body::Body::empty())
        .expect("build request");

    let err = engine
        .handle_request(request)
        .await
        .expect_err("handle request should fail");
    assert!(format!("{err}").contains("payload too large"));

    let items = wait_for_trace_items(rules_dir).await;
    assert!(!items.is_empty());
    assert!(items.iter().any(|item| item.status == "error"));

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
}
