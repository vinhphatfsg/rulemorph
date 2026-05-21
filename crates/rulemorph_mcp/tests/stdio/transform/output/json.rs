#[test]
fn transform_json_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.yaml");
    let input_path = dir.path().join("input.json");

    fs::write(
        &rules_path,
        r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("write rules");
    fs::write(&input_path, r#"{"id": 1}"#).expect("write input");

    let request = tool_call_request(
        3,
        "transform",
        json!({
            "rules_path": rules_path.to_string_lossy(),
            "input_path": input_path.to_string_lossy()
        }),
    );

    let response = server.send(&request);
    let output = content_json(&response);

    assert_eq!(output, json!([{ "id": 1 }]));
    assert!(response["result"]["isError"].is_null() || response["result"]["isError"] == false);

    server.shutdown();
}

#[test]
fn transform_accepts_json_rules_text_with_rules_format() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = tool_call_request(
        33,
        "transform",
        json!({
            "rules_text": r#"{
              "version": 2,
              "input": { "format": "json", "json": {} },
              "mappings": [{ "target": "id", "source": "id" }]
            }"#,
            "rules_format": "json",
            "input_json": { "id": 1 },
            "return_output_json": true
        }),
    );

    let response = server.send(&request);
    assert!(response.get("error").is_none(), "response: {response}");
    assert_eq!(response["result"]["meta"]["output"], json!([{ "id": 1 }]));
    server.shutdown();
}
