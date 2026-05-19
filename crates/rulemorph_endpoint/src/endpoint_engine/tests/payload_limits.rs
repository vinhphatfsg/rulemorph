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

#[tokio::test]
async fn request_body_read_error_returns_network_error() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    std::fs::write(
        rules_dir.join("endpoint.yaml"),
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
    )
    .expect("write endpoint.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let stream = stream::once(async {
        Err::<axum::body::Bytes, std::io::Error>(std::io::Error::new(
            std::io::ErrorKind::Other,
            "boom",
        ))
    });
    let body = axum::body::Body::from_stream(stream);
    let request = Request::builder()
        .method("POST")
        .uri("/api/test")
        .body(body)
        .expect("build request");

    let err = engine
        .handle_request(request)
        .await
        .expect_err("handle request should fail");
    assert!(format!("{err}").contains("request body read error"));
}

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

#[tokio::test]
async fn network_chunked_response_too_large_returns_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let host = format!("localhost:{}", addr.port());

    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 1024];
        let _ = socket.read(&mut buf).await;
        let headers = concat!(
            "HTTP/1.1 200 OK\r\n",
            "content-type: application/json\r\n",
            "transfer-encoding: chunked\r\n",
            "\r\n"
        );
        let _ = socket.write_all(headers.as_bytes()).await;

        let chunk1 = "{\"data\":\"";
        let chunk2 = format!("{}\"}}", "x".repeat(64));
        let chunk1_line = format!("{:X}\r\n{}\r\n", chunk1.len(), chunk1);
        let chunk2_line = format!("{:X}\r\n{}\r\n", chunk2.len(), chunk2);
        let _ = socket.write_all(chunk1_line.as_bytes()).await;
        let _ = socket.write_all(chunk2_line.as_bytes()).await;
        let _ = socket.write_all(b"0\r\n\r\n").await;
        let _ = socket.flush().await;
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
            .with_max_response_bytes(32),
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

    let _ = server_handle.await;
}
