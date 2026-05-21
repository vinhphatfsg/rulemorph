#[test]
fn tools_call_unknown_tool_returns_tool_error_payload() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 6,
        "method": "tools/call",
        "params": {
            "name": "unknown_tool",
            "arguments": {}
        }
    });

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    assert_eq!(
        response["result"]["content"][0],
        json!({
            "type": "text",
            "text": "unknown tool: unknown_tool"
        })
    );
    assert!(response["result"]["meta"].is_null());

    server.shutdown();
}

#[test]
fn validate_rules_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#;

    let request = tool_call_request(
        8,
        "validate_rules",
        json!({
            "rules_text": rules_text
        }),
    );

    let response = server.send(&request);
    assert_eq!(content_text(&response), "ok");

    server.shutdown();
}

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
