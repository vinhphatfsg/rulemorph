#[test]
fn transform_rejects_paths_outside_allowed_root() {
    let allowed = tempdir().expect("allowed temp dir");
    let outside = tempdir().expect("outside temp dir");
    let rules_path = outside.path().join("rules.yaml");
    let input_path = outside.path().join("input.json");

    fs::write(
        &rules_path,
        r#"version: 1
input:
  format: json
  json: {}
mappings: []
"#,
    )
    .expect("write rules");
    fs::write(&input_path, r#"{"id": 1}"#).expect("write input");

    let mut server = McpServer::start_with_allowed_root(allowed.path());
    initialize(&mut server);
    let request = json!({
        "jsonrpc": "2.0",
        "id": 30,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": rules_path.to_string_lossy(),
                "input_path": input_path.to_string_lossy()
            }
        }
    });

    let response = server.send(&request);
    let message = response["result"]["content"][0]["text"]
        .as_str()
        .expect("error text");
    assert!(message.contains("outside MCP allowed roots"));
    server.shutdown();
}
