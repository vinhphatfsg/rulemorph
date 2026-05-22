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
