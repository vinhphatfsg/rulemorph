#[test]
fn transform_accepts_excel_input_path_for_json_and_ndjson_output() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let fixture_dir = core_fixtures_dir().join("t34_excel_input");
    let rules_path = fixture_dir.join("rules.yaml");
    let input_path = fixture_dir.join("input.xlsx");
    let expected_text =
        fs::read_to_string(fixture_dir.join("expected.json")).expect("read expected Excel output");
    let expected: Value = serde_json::from_str(&expected_text).expect("expected JSON");

    let json_request = json!({
        "jsonrpc": "2.0",
        "id": 150,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": rules_path.to_string_lossy(),
                "input_path": input_path.to_string_lossy(),
                "return_output_json": true
            }
        }
    });
    let json_response = server.send(&json_request);
    assert!(
        json_response.get("error").is_none(),
        "response: {json_response}"
    );
    assert_eq!(json_response["result"]["meta"]["output"], expected);

    let ndjson_request = json!({
        "jsonrpc": "2.0",
        "id": 151,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": rules_path.to_string_lossy(),
                "input_path": input_path.to_string_lossy(),
                "ndjson": true
            }
        }
    });
    let ndjson_response = server.send(&ndjson_request);
    assert!(
        ndjson_response.get("error").is_none(),
        "response: {ndjson_response}"
    );
    let output_text = ndjson_response["result"]["content"][0]["text"]
        .as_str()
        .expect("ndjson output text");
    let rows = output_text
        .trim_end_matches('\n')
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("ndjson row"))
        .collect::<Vec<_>>();
    assert_eq!(Value::Array(rows), expected);

    server.shutdown();
}
