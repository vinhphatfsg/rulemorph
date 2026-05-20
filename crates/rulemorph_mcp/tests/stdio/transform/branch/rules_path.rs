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
