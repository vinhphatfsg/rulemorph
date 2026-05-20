#[test]
fn raw_json_input_text_rejects_duplicate_keys_for_analysis_and_generation() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let duplicate_input = r#"{"items":[{"id":1,"id":2}]}"#;
    let base_rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "old_id"
"#;
    let dto_text = r#"export interface Record {
  id: string;
}"#;

    let cases = [
        json!({
            "jsonrpc": "2.0",
            "id": 1201,
            "method": "tools/call",
            "params": {
                "name": "analyze_input",
                "arguments": {
                    "input_text": duplicate_input,
                    "format": "json"
                }
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 1202,
            "method": "tools/call",
            "params": {
                "name": "generate_rules_from_base",
                "arguments": {
                    "rules_text": base_rules_text,
                    "input_text": duplicate_input,
                    "format": "json"
                }
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 1203,
            "method": "tools/call",
            "params": {
                "name": "generate_rules_from_dto",
                "arguments": {
                    "dto_text": dto_text,
                    "dto_language": "typescript",
                    "input_text": duplicate_input,
                    "format": "json"
                }
            }
        }),
    ];

    for request in cases {
        let response = server.send(&request);
        assert_eq!(response["result"]["isError"], true);
        let message = response["result"]["content"][0]["text"]
            .as_str()
            .expect("error text");
        assert!(message.contains("duplicate key"), "{message}");
    }

    server.shutdown();
}
