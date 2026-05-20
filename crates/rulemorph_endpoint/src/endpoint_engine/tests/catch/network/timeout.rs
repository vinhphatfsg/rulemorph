#[tokio::test]
async fn network_timeout_on_slow_body_runs_catch() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let host = format!("localhost:{}", addr.port());

    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 1024];
        let _ = socket.read(&mut buf).await;
        let body = b"{\"value\":1}";
        let headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        let _ = socket.write_all(headers.as_bytes()).await;
        let _ = socket.flush().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = socket.write_all(body).await;
        let _ = socket.shutdown().await;
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
  url: "http://{}/slow"
timeout: 100ms
catch:
  timeout: ./catch.yaml
"#,
            host
        ),
    )
    .expect("write network.yaml");

    std::fs::write(
        rules_subdir.join("catch.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "handled"
    value: true
"#,
    )
    .expect("write catch.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let request = Request::builder()
        .method("GET")
        .uri("/api/test")
        .body(axum::body::Body::empty())
        .expect("build request");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_eq!(response.status().as_u16(), 200);

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
    assert_eq!(body, json!({ "handled": true }));

    let _ = server_handle.await;
}
