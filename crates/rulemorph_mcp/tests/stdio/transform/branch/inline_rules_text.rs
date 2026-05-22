#[test]
fn transform_rejects_rules_text_file_branch() {
    let dir = tempdir().expect("allowed temp dir");
    let mut server = McpServer::start_with_allowed_root(dir.path());
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 32,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_text": "version: 2\ninput:\n  format: json\n  json: {}\nsteps:\n  - branch:\n      when: { eq: [1, 1] }\n      then: /tmp/outside.yaml\n",
                "input_json": { "id": 1 }
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["error"]["code"], -32602);
    assert!(
        response["error"]["message"]
            .as_str()
            .expect("error message")
            .contains("rules_text cannot use branch")
    );
    server.shutdown();
}

#[test]
fn transform_rejects_json_rules_text_file_branch() {
    let dir = tempdir().expect("allowed temp dir");
    let mut server = McpServer::start_with_allowed_root(dir.path());
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 34,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_text": r#"{
                  "version": 2,
                  "input": { "format": "json", "json": {} },
                  "steps": [{
                    "branch": {
                      "when": { "eq": [1, 1] },
                      "then": "/tmp/outside.json"
                    }
                  }]
                }"#,
                "rules_format": "json",
                "input_json": { "id": 1 }
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["error"]["code"], -32602);
    assert!(
        response["error"]["message"]
            .as_str()
            .expect("error message")
            .contains("rules_text cannot use branch")
    );
    server.shutdown();
}
