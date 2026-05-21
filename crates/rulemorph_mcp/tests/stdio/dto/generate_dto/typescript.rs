#[test]
fn generate_dto_typescript() {
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
        10,
        "generate_dto",
        json!({
            "rules_text": rules_text,
            "language": "typescript"
        }),
    );

    let response = server.send(&request);
    assert!(content_text(&response).contains("export interface"));

    server.shutdown();
}
