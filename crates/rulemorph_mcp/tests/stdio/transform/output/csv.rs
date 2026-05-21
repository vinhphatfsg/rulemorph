#[test]
fn transform_csv_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.yaml");
    let input_path = dir.path().join("input.csv");

    fs::write(
        &rules_path,
        r#"version: 1
input:
  format: csv
  csv: {}
mappings:
  - target: "name"
    source: "name"
  - target: "age"
    source: "age"
"#,
    )
    .expect("write rules");
    fs::write(&input_path, "name,age\nAlice,30\nBob,25\n").expect("write input");

    let request = json!({
        "jsonrpc": "2.0",
        "id": 7,
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
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let output: Value = serde_json::from_str(output_text).expect("output json");

    assert_eq!(
        output,
        json!([
            { "name": "Alice", "age": "30" },
            { "name": "Bob", "age": "25" }
        ])
    );

    server.shutdown();
}
