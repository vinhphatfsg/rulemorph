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

#[test]
fn transform_accepts_text_input_normalization_formats() {
    let cases = [
        (
            "yaml",
            r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
            "users:\n  - id: \"1\"\n    name: Alice\n",
        ),
        (
            "toml",
            r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
            "[[users]]\nid = \"1\"\nname = \"Alice\"\n",
        ),
        (
            "xml",
            r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    attr_prefix: "@"
    text_key: "#text"
mappings:
  - target: "id"
    source: 'input.["@id"]'
  - target: "name"
    source: 'input.name[0]["#text"]'
"##,
            r#"<users><user id="1"><name>Alice</name></user></users>"#,
        ),
        (
            "html",
            r#"
version: 2
input:
  format: html
  html:
    records_selector: "table#users tbody tr"
    fields:
      id:
        selector: "td:nth-child(1)"
        value: text
      name:
        selector: "td:nth-child(2)"
        value: text
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
            r#"<table id="users"><tbody><tr><td>1</td><td>Alice</td></tr></tbody></table>"#,
        ),
    ];

    for (index, (format, rules_text, input_text)) in cases.into_iter().enumerate() {
        let mut server = McpServer::start();
        initialize(&mut server);
        let request = json!({
            "jsonrpc": "2.0",
            "id": 100 + index,
            "method": "tools/call",
            "params": {
                "name": "transform",
                "arguments": {
                    "rules_text": rules_text,
                    "input_text": input_text,
                    "format": format,
                    "return_output_json": true
                }
            }
        });
        let response = server.send(&request);
        assert!(response.get("error").is_none(), "response: {response}");
        assert_eq!(
            response["result"]["meta"]["output"],
            json!([{ "id": "1", "name": "Alice" }])
        );
        server.shutdown();
    }
}

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
