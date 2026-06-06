#[test]
fn validate_rules_success_includes_rule_warnings() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "formatted"
    expr:
      op: "date_format"
      args:
        - { ref: "input.date" }
        - "%Y-%m-%d"
  - target: "epoch"
    expr:
      op: "to_unixtime"
      args:
        - { ref: "input.date" }
  - target: "chain_formatted"
    expr:
      chain:
        - { ref: "input.date" }
        - op: "date_format"
          args:
            - "%Y-%m-%d"
"#;

    let request = tool_call_request(
        9,
        "validate_rules",
        json!({
            "rules_text": rules_text
        }),
    );

    let response = server.send(&request);
    assert_eq!(content_text(&response), "ok");
    assert_eq!(
        response["result"]["meta"]["warnings"],
        json!([
            {
                "type": "warning",
                "code": "date_format_missing_input_format",
                "message": "date_format without input_format relies on heuristic parsing; consider providing input_format.",
                "path": "mappings[0].expr.args"
            },
            {
                "type": "warning",
                "code": "to_unixtime_auto_parse",
                "message": "to_unixtime relies on heuristic date parsing; consider normalizing with date_format + input_format.",
                "path": "mappings[1].expr"
            },
            {
                "type": "warning",
                "code": "date_format_missing_input_format",
                "message": "date_format without input_format relies on heuristic parsing; consider providing input_format.",
                "path": "mappings[2].expr.chain[1].args"
            }
        ])
    );

    server.shutdown();
}

#[test]
fn validate_rules_failure() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: csv
mappings: []
"#;

    let request = tool_call_request(
        9,
        "validate_rules",
        json!({
            "rules_text": rules_text
        }),
    );

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    assert!(response["result"]["meta"]["errors"].is_array());

    server.shutdown();
}

#[test]
fn validate_rules_path_rejects_missing_branch_child() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.yaml");
    fs::write(
        &rules_path,
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: ./missing.yaml
      return: false
  - mappings:
      - target: after
        expr: "@out.branch_value"
"#,
    )
    .expect("write rules");

    let response = call_tool(
        &mut server,
        10,
        "validate_rules",
        json!({
            "rules_path": rules_path.to_string_lossy()
        }),
    );
    assert_eq!(response["result"]["isError"], true);
    let errors = response["result"]["meta"]["errors"]
        .as_array()
        .expect("validation errors");
    assert!(
        errors.iter().any(|err| err["code"] == "InvalidStep"
            && err["message"]
                .as_str()
                .expect("error message")
                .contains("failed to resolve branch rule")),
        "missing branch child should be reported by path-based validation: {errors:?}"
    );

    server.shutdown();
}
