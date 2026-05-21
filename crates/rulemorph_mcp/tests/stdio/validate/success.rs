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
