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
