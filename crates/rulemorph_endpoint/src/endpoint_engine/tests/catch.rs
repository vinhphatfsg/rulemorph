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

include!("catch/request.rs");
include!("catch/network.rs");
