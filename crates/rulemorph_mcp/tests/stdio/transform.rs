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

#[test]
fn transform_allows_new_output_directory_inside_allowed_root() {
    let dir = tempdir().expect("allowed temp dir");
    let rules_path = dir.path().join("rules.yaml");
    let input_path = dir.path().join("input.json");
    let output_path = dir.path().join("new").join("out.json");

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

    let mut server = McpServer::start_with_allowed_root(dir.path());
    initialize(&mut server);
    let request = json!({
        "jsonrpc": "2.0",
        "id": 31,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": rules_path.to_string_lossy(),
                "input_path": input_path.to_string_lossy(),
                "output_path": output_path.to_string_lossy()
            }
        }
    });

    let response = server.send(&request);
    assert!(response["result"]["isError"].is_null() || response["result"]["isError"] == false);
    let output_file = fs::read_to_string(&output_path).expect("read output file");
    let output: Value = serde_json::from_str(&output_file).expect("output json");
    assert_eq!(output, json!([{ "id": 1 }]));
    server.shutdown();
}

#[test]
fn transform_rejects_output_path_traversal_outside_allowed_root() {
    let dir = tempdir().expect("allowed temp dir");
    let rules_path = dir.path().join("rules.yaml");
    let input_path = dir.path().join("input.json");
    let outside_dir = tempfile::Builder::new()
        .prefix("outside-")
        .tempdir_in(dir.path().parent().expect("temp parent"))
        .expect("outside temp dir");
    let outside_name = outside_dir
        .path()
        .file_name()
        .expect("outside dir name")
        .to_owned();
    outside_dir.close().expect("remove outside marker");
    let output_path = dir.path().join("..").join(outside_name).join("out.json");

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

    let mut server = McpServer::start_with_allowed_root(dir.path());
    initialize(&mut server);
    let request = json!({
        "jsonrpc": "2.0",
        "id": 42,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": rules_path.to_string_lossy(),
                "input_path": input_path.to_string_lossy(),
                "output_path": output_path.to_string_lossy()
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    let message = response["result"]["content"][0]["text"]
        .as_str()
        .expect("error text");
    assert!(message.contains("outside MCP allowed roots"));
    assert!(!output_path.exists());
    server.shutdown();
}

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

#[test]
fn transform_rules_path_resolves_branch_relative_paths() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.yaml");
    let then_path = dir.path().join("branch_child.yaml");
    let else_path = dir.path().join("branch_else.yaml");
    let input_path = dir.path().join("input.json");

    fs::write(
        &rules_path,
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: ["@input.kind", "child"] }
      then: ./branch_child.yaml
      else: ./branch_else.yaml
      return: true
"#,
    )
    .expect("write rules");
    fs::write(
        &then_path,
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: "result"
    value: "child"
"#,
    )
    .expect("write then rules");
    fs::write(
        &else_path,
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: "result"
    value: "else"
"#,
    )
    .expect("write else rules");
    fs::write(&input_path, r#"[{"kind": "child"}, {"kind": "other"}]"#).expect("write input");

    let request = json!({
        "jsonrpc": "2.0",
        "id": 25,
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

    assert_eq!(output, json!([{ "result": "child" }, { "result": "else" }]));
    assert!(response["result"]["isError"].is_null() || response["result"]["isError"] == false);

    server.shutdown();
}

#[test]
fn tools_call_invalid_params_returns_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "transform"
        }
    });

    let response = server.send(&request);
    assert_eq!(response["error"]["code"], -32602);

    server.shutdown();
}

#[test]
fn tools_call_invalid_argument_type_returns_json_rpc_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 41,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_text": "version: 1\ninput:\n  format: json\n  json: {}\nmappings: []",
                "input_json": { "id": 1 },
                "ndjson": "yes"
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(response["error"]["message"], "ndjson must be a boolean");

    server.shutdown();
}

#[test]
fn tools_call_missing_files_returns_tool_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": "nope.yaml",
                "input_path": "nope.json"
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    let message = response["result"]["content"][0]["text"]
        .as_str()
        .expect("error text");
    assert!(message.contains("failed to read rules"));

    server.shutdown();
}

#[test]
fn ndjson_and_output_path() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.yaml");
    let input_path = dir.path().join("input.json");
    let output_path = dir.path().join("out.ndjson");

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
    fs::write(&input_path, r#"[{"id": 1}, {"id": 2}]"#).expect("write input");

    let request = json!({
        "jsonrpc": "2.0",
        "id": 6,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": rules_path.to_string_lossy(),
                "input_path": input_path.to_string_lossy(),
                "ndjson": true,
                "output_path": output_path.to_string_lossy()
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let lines: Vec<&str> = output_text.trim_end_matches('\n').split('\n').collect();

    assert_eq!(lines.len(), 2);
    let first: Value = serde_json::from_str(lines[0]).expect("line 1 json");
    let second: Value = serde_json::from_str(lines[1]).expect("line 2 json");
    assert_eq!(first, json!({"id": 1}));
    assert_eq!(second, json!({"id": 2}));

    let output_file = fs::read_to_string(&output_path).expect("read output file");
    assert_eq!(output_file, output_text);

    server.shutdown();
}

#[test]
fn ndjson_rules_path_resolves_branch_relative_paths() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.yaml");
    let then_path = dir.path().join("branch_child.yaml");
    let else_path = dir.path().join("branch_else.yaml");
    let input_path = dir.path().join("input.json");

    fs::write(
        &rules_path,
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: ["@input.kind", "child"] }
      then: ./branch_child.yaml
      else: ./branch_else.yaml
      return: true
"#,
    )
    .expect("write rules");
    fs::write(
        &then_path,
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: "result"
    value: "child"
"#,
    )
    .expect("write then rules");
    fs::write(
        &else_path,
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: "result"
    value: "else"
"#,
    )
    .expect("write else rules");
    fs::write(&input_path, r#"[{"kind": "child"}, {"kind": "other"}]"#).expect("write input");

    let request = json!({
        "jsonrpc": "2.0",
        "id": 26,
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

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let lines: Vec<&str> = output_text.trim_end_matches('\n').split('\n').collect();

    assert_eq!(lines.len(), 2);
    let first: Value = serde_json::from_str(lines[0]).expect("line 1 json");
    let second: Value = serde_json::from_str(lines[1]).expect("line 2 json");
    assert_eq!(first, json!({"result": "child"}));
    assert_eq!(second, json!({"result": "else"}));
    assert!(response["result"]["isError"].is_null() || response["result"]["isError"] == false);

    server.shutdown();
}

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
