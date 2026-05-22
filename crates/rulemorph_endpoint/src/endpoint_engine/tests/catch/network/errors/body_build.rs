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
