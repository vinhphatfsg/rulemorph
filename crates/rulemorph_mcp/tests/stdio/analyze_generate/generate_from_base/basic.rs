#[test]
fn generate_rules_from_base_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "old_id"
  - target: "name"
    source: "old_name"
"#;

    let rule = call_tool_rule(
        &mut server,
        14,
        "generate_rules_from_base",
        json!({
            "rules_text": rules_text,
            "input_json": {
                "id": 1,
                "name": "Ada"
            }
        }),
    );
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));

    server.shutdown();
}
