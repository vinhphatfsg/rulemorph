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
