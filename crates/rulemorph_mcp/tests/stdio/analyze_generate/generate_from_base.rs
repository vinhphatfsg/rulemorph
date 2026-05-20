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

#[test]
fn generate_rules_from_base_csv_uses_scalar_type_boost() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: csv
  csv: {}
mappings:
  - target: "price"
    source: "old_price"
    type: float
"#;

    let rule = call_tool_rule(
        &mut server,
        141,
        "generate_rules_from_base",
        json!({
            "rules_text": rules_text,
            "input_text": "price_text,price_value\nabc,12.5\n",
            "format": "csv"
        }),
    );
    assert_eq!(rule.mappings[0].source.as_deref(), Some("price_value"));

    server.shutdown();
}
