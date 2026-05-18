#[tokio::test]
async fn step_catch_inherits_with_params() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/failing_network.yaml
        with:
          fields: ["name"]
        catch:
          default: ./rules/catch.yaml
    reply:
      status: 200
      body: "@input"
"#,
    );

    std::fs::write(
        rules_subdir.join("failing_network.yaml"),
        r#"
version: 2
type: network
request:
  method: GET
  url: "http://example.com"
timeout: 1s
body: "@input"
"#,
    )
    .expect("write failing network rule");

    std::fs::write(
        rules_subdir.join("catch.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "params"
    expr: "@context.params"
    required: true
"#,
    )
    .expect("write catch rule");

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
    assert_eq!(body, json!({ "params": { "fields": ["name"] } }));
}

#[tokio::test]
async fn endpoint_duplicate_query_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("GET", "/api/test?dup=1&dup=2");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn endpoint_invalid_json_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = Request::builder()
        .method("POST")
        .uri("/api/test")
        .header("content-type", "application/json")
        .body(axum::body::Body::from("{\"bad\":}"))
        .expect("build request");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn endpoint_invalid_json_keeps_query_in_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "query"
    expr: "@input.query"
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = Request::builder()
        .method("POST")
        .uri("/api/test?token=abc")
        .header("content-type", "application/json")
        .body(axum::body::Body::from("{\"bad\":}"))
        .expect("build request");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "query": { "token": "abc" } })).await;
}

#[tokio::test]
async fn endpoint_input_mapping_error_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    input:
      - target: "user_id"
        source: "input.body.user_id"
        required: true
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("POST", "/api/test");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn reply_eval_error_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: "@input.status"
      body: "@input.body"
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "status"
    value: 200
  - target: "body"
    value:
      handled: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("GET", "/api/test");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn network_url_eval_error_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
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
    );

    write_rule_yaml(
        &rules_subdir,
        "network.yaml",
        r#"
version: 2
type: network
request:
  method: GET
  url: "@input.url"
timeout: 1s
catch:
  default: ./catch.yaml
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("GET", "/api/test");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn network_body_build_error_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
    );

    write_rule_yaml(
        &rules_subdir,
        "network.yaml",
        r#"
version: 2
type: network
request:
  method: POST
  url: "https://example.com"
timeout: 1s
body_map:
  - target: "required"
    source: "input.missing"
    required: true
catch:
  default: ./catch.yaml
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("POST", "/api/test");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn network_select_error_runs_catch() {
    let app = axum::Router::new().route(
        "/data",
        axum::routing::get(|| async { axum::Json(json!({ "data": { "value": 1 } })) }),
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
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
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
    );

    write_rule_yaml(
        &rules_subdir,
        "network.yaml",
        &format!(
            r#"
version: 2
type: network
request:
  method: GET
  url: "http://{}/data"
timeout: 1s
select: "missing.path"
catch:
  default: ./catch.yaml
"#,
            host
        ),
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("GET", "/api/test");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
}

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
