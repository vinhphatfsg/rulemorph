#[test]
fn transform_csv_input_text_keeps_cell_values_as_strings() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
  - target: "flag"
    source: "flag"
  - target: "empty"
    source: "empty"
"#;

    let request = tool_call_request(
        32,
        "transform",
        json!({
            "rules_text": rules_text,
            "input_text": "id,flag,empty\n001,true,\n",
            "format": "csv"
        }),
    );

    let response = server.send(&request);
    let output = content_json(&response);
    assert_eq!(
        output,
        json!([{ "id": "001", "flag": "true", "empty": "" }])
    );

    server.shutdown();
}
