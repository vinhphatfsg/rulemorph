use std::fs;

use rulemorph::parse_rule_file;
use serde_json::{Value, json};
use tempfile::tempdir;

mod common;

use common::stdio::{McpServer, core_fixtures_dir, initialize};

#[test]
fn initialize_and_list_tools() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list"
    });
    let response = server.send(&request);

    let tools = response["result"]["tools"].as_array().expect("tools array");
    let expected = [
        "transform",
        "validate_rules",
        "generate_dto",
        "list_ops",
        "analyze_input",
        "generate_rules_from_base",
        "generate_rules_from_dto",
    ];
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect::<Vec<_>>();
    assert_eq!(names, expected);
    for name in expected {
        assert!(tools.iter().any(|tool| tool["name"] == name));
    }
    for name in [
        "transform",
        "validate_rules",
        "generate_dto",
        "generate_rules_from_base",
    ] {
        let tool = tools
            .iter()
            .find(|tool| tool["name"] == name)
            .expect("tool");
        assert_eq!(
            tool["inputSchema"]["properties"]["rules_format"]["enum"],
            json!(["yaml", "json"]),
            "rules_format schema missing for {name}"
        );
    }
    let transform_tool = tools
        .iter()
        .find(|tool| tool["name"] == "transform")
        .expect("transform tool");
    assert_eq!(
        transform_tool["inputSchema"]["properties"]["format"]["enum"],
        json!(["csv", "json", "yaml", "toml", "xml", "html", "excel"])
    );
    let input_json_description =
        transform_tool["inputSchema"]["properties"]["input_json"]["description"]
            .as_str()
            .expect("input_json description");
    assert!(input_json_description.contains("Inline typed JSON value"));
    assert!(input_json_description.contains("Duplicate-key validation"));

    let generate_dto_tool = tools
        .iter()
        .find(|tool| tool["name"] == "generate_dto")
        .expect("generate_dto tool");
    assert_eq!(
        generate_dto_tool["inputSchema"]["required"],
        json!(["language"])
    );
    assert_eq!(
        generate_dto_tool["inputSchema"]["properties"]["language"]["enum"],
        json!([
            "rust",
            "typescript",
            "python",
            "go",
            "java",
            "kotlin",
            "swift"
        ])
    );

    let list_ops_tool = tools
        .iter()
        .find(|tool| tool["name"] == "list_ops")
        .expect("list_ops tool");
    assert_eq!(list_ops_tool["inputSchema"]["properties"], json!({}));

    let analyze_input_tool = tools
        .iter()
        .find(|tool| tool["name"] == "analyze_input")
        .expect("analyze_input tool");
    assert_eq!(
        analyze_input_tool["inputSchema"]["properties"]["format"]["enum"],
        json!(["csv", "json"])
    );
    assert_eq!(
        analyze_input_tool["inputSchema"]["properties"]["max_paths"]["minimum"],
        json!(1)
    );

    let generate_rules_from_base_tool = tools
        .iter()
        .find(|tool| tool["name"] == "generate_rules_from_base")
        .expect("generate_rules_from_base tool");
    assert_eq!(
        generate_rules_from_base_tool["inputSchema"]["properties"]["max_candidates"]["minimum"],
        json!(1)
    );
    assert!(
        generate_rules_from_base_tool["inputSchema"]["properties"]
            .as_object()
            .expect("properties")
            .contains_key("records_path")
    );

    let generate_rules_from_dto_tool = tools
        .iter()
        .find(|tool| tool["name"] == "generate_rules_from_dto")
        .expect("generate_rules_from_dto tool");
    assert_eq!(
        generate_rules_from_dto_tool["inputSchema"]["required"],
        json!(["dto_text", "dto_language"])
    );
    assert_eq!(
        generate_rules_from_dto_tool["inputSchema"]["properties"]["dto_language"]["enum"],
        json!([
            "rust",
            "typescript",
            "python",
            "go",
            "java",
            "kotlin",
            "swift"
        ])
    );

    server.shutdown();
}

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

    let request = json!({
        "jsonrpc": "2.0",
        "id": 3,
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

    let request = json!({
        "jsonrpc": "2.0",
        "id": 32,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_text": rules_text,
                "input_text": "id,flag,empty\n001,true,\n",
                "format": "csv"
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

    let request = json!({
        "jsonrpc": "2.0",
        "id": 33,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_text": r#"{
                  "version": 2,
                  "input": { "format": "json", "json": {} },
                  "mappings": [{ "target": "id", "source": "id" }]
                }"#,
                "rules_format": "json",
                "input_json": { "id": 1 },
                "return_output_json": true
            }
        }
    });

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

#[test]
fn tools_call_unknown_tool_returns_tool_error_payload() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 6,
        "method": "tools/call",
        "params": {
            "name": "unknown_tool",
            "arguments": {}
        }
    });

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    assert_eq!(
        response["result"]["content"][0],
        json!({
            "type": "text",
            "text": "unknown tool: unknown_tool"
        })
    );
    assert!(response["result"]["meta"].is_null());

    server.shutdown();
}

#[test]
fn validate_rules_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 8,
        "method": "tools/call",
        "params": {
            "name": "validate_rules",
            "arguments": {
                "rules_text": rules_text
            }
        }
    });

    let response = server.send(&request);
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("result text");
    assert_eq!(text, "ok");

    server.shutdown();
}

#[test]
fn validate_rules_failure() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: csv
mappings: []
"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 9,
        "method": "tools/call",
        "params": {
            "name": "validate_rules",
            "arguments": {
                "rules_text": rules_text
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    assert!(response["result"]["meta"]["errors"].is_array());

    server.shutdown();
}

#[test]
fn generate_dto_typescript() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 10,
        "method": "tools/call",
        "params": {
            "name": "generate_dto",
            "arguments": {
                "rules_text": rules_text,
                "language": "typescript"
            }
        }
    });

    let response = server.send(&request);
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("dto text");
    assert!(text.contains("export interface"));

    server.shutdown();
}

#[test]
fn generate_dto_invalid_language_returns_json_rpc_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 101,
        "method": "tools/call",
        "params": {
            "name": "generate_dto",
            "arguments": {
                "rules_text": rules_text,
                "language": "ruby"
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(
        response["error"]["message"],
        "language must be one of rust, typescript, python, go, java, kotlin, swift"
    );

    server.shutdown();
}

#[test]
fn list_ops_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 11,
        "method": "tools/call",
        "params": {
            "name": "list_ops",
            "arguments": {}
        }
    });

    let response = server.send(&request);
    assert!(response["result"]["meta"]["ops"]["type_casts"].is_array());
    assert!(response["result"]["meta"]["ops"]["categories"]["json_ops"].is_array());
    assert!(response["result"]["meta"]["ops"]["categories"]["array_ops"].is_array());
    assert!(response["result"]["meta"]["ops"]["category_docs"]["json_ops"]["examples"].is_array());
    assert!(
        response["result"]["meta"]["ops"]["category_docs"]["string_ops"]["examples"].is_array()
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["logical_ops"],
        json!(["and", "or", "not"])
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["comparison_ops"],
        json!(["==", "!=", "<", "<=", ">", ">=", "~="])
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["type_casts"],
        json!(["string", "int", "float", "bool"])
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["categories"]["numeric_ops"],
        json!([
            "+", "-", "*", "/", "round", "to_base", "sum", "avg", "min", "max"
        ])
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["categories"]["date_ops"],
        json!(["date_format", "to_unixtime"])
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["expr_ops"],
        json!([
            "concat",
            "coalesce",
            "to_string",
            "trim",
            "lowercase",
            "uppercase",
            "replace",
            "split",
            "pad_start",
            "pad_end",
            "lookup",
            "lookup_first",
            "merge",
            "deep_merge",
            "get",
            "pick",
            "omit",
            "keys",
            "values",
            "entries",
            "len",
            "from_entries",
            "object_flatten",
            "object_unflatten",
            "map",
            "filter",
            "flat_map",
            "flatten",
            "take",
            "drop",
            "slice",
            "chunk",
            "zip",
            "zip_with",
            "unzip",
            "group_by",
            "key_by",
            "partition",
            "unique",
            "distinct_by",
            "sort_by",
            "find",
            "find_index",
            "index_of",
            "contains",
            "sum",
            "avg",
            "min",
            "max",
            "reduce",
            "fold",
            "+",
            "-",
            "*",
            "/",
            "round",
            "to_base",
            "date_format",
            "to_unixtime"
        ])
    );
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("ops text");
    let text_ops: Value = serde_json::from_str(text).expect("ops text json");
    assert_eq!(text_ops, response["result"]["meta"]["ops"]);

    server.shutdown();
}

#[test]
fn analyze_input_json_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 12,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_json": {
                    "id": 1,
                    "name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let paths = response["result"]["meta"]["paths"]
        .as_array()
        .expect("paths array");
    assert!(paths.iter().any(|item| item["path"] == "id"));
    assert!(paths.iter().any(|item| item["path"] == "name"));

    server.shutdown();
}

#[test]
fn analyze_input_records_path_supports_quoted_segments() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 121,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_json": {
                    "payload": {
                        "items.with.dot": [
                            {
                                "user.name": "Ada",
                                "age": 37
                            }
                        ]
                    }
                },
                "records_path": "payload[\"items.with.dot\"]"
            }
        }
    });

    let response = server.send(&request);
    let paths = response["result"]["meta"]["paths"]
        .as_array()
        .expect("paths array");
    assert!(paths.iter().any(|item| item["path"] == "[\"user.name\"]"));
    assert!(paths.iter().any(|item| item["path"] == "age"));
    assert_eq!(response["result"]["meta"]["summary"]["records"], json!(1));

    server.shutdown();
}

#[test]
fn analyze_input_invalid_records_path_returns_json_rpc_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 122,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_json": {
                    "payload": [
                        { "id": 1 }
                    ]
                },
                "records_path": "payload."
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(
        response["error"]["message"],
        "records_path is invalid: path syntax is invalid"
    );

    server.shutdown();
}

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

#[test]
fn analyze_input_csv_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 13,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_text": "id,active,name,score,empty\n1,true,Ada,3.5,\n2,false,Bob,4,\n",
                "format": "csv"
            }
        }
    });

    let response = server.send(&request);
    let paths = response["result"]["meta"]["paths"]
        .as_array()
        .expect("paths array");
    assert!(paths.iter().any(|item| item["path"] == "id"));
    let id_path = paths
        .iter()
        .find(|item| item["path"] == "id")
        .expect("id path");
    assert_eq!(id_path["types"]["number"], json!(2));
    let active_path = paths
        .iter()
        .find(|item| item["path"] == "active")
        .expect("active path");
    assert_eq!(active_path["types"]["bool"], json!(2));
    let empty_path = paths
        .iter()
        .find(|item| item["path"] == "empty")
        .expect("empty path");
    assert_eq!(empty_path["types"]["null"], json!(2));

    server.shutdown();
}

#[test]
fn analyze_input_csv_rejects_mismatched_field_count() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 131,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_text": "id,name\n1,Ada,extra\n",
                "format": "csv"
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    let message = response["result"]["content"][0]["text"]
        .as_str()
        .expect("error text");
    assert!(message.contains("failed to parse input CSV"), "{message}");

    server.shutdown();
}

#[test]
fn analyze_input_csv_allows_more_than_default_normalization_record_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_path = dir.path().join("large.csv");
    let mut input_text = String::from("id\n");
    for index in 0..100_001 {
        input_text.push_str(&index.to_string());
        input_text.push('\n');
    }
    std::fs::write(&input_path, input_text).expect("write csv");

    let mut server = McpServer::start_with_allowed_root(dir.path());
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 132,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_path": input_path.to_string_lossy(),
                "format": "csv",
                "max_paths": 1
            }
        }
    });

    let response = server.send(&request);
    assert_ne!(response["result"]["isError"], json!(true));
    assert_eq!(
        response["result"]["meta"]["summary"]["records"],
        json!(100_001)
    );

    server.shutdown();
}

#[test]
fn generate_rules_from_base_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "old_id"
  - target: "name"
    source: "old_name"
"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 14,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_base",
            "arguments": {
                "rules_text": rules_text,
                "input_json": {
                    "id": 1,
                    "name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));

    server.shutdown();
}

#[test]
fn generate_rules_from_base_csv_uses_scalar_type_boost() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: csv
  csv: {}
mappings:
  - target: "price"
    source: "old_price"
    type: float
"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 141,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_base",
            "arguments": {
                "rules_text": rules_text,
                "input_text": "price_text,price_value\nabc,12.5\n",
                "format": "csv"
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");
    assert_eq!(rule.mappings[0].source.as_deref(), Some("price_value"));

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"export interface Record {
  id: string;
  name?: string;
}"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 15,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "typescript",
                "input_json": {
                    "id": 1,
                    "name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_invalid_language_returns_json_rpc_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 1510,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": "type Record = { id: string }",
                "dto_language": "ruby",
                "input_json": {
                    "id": 1
                }
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(
        response["error"]["message"],
        "dto_language must be rust, typescript, python, go, java, kotlin, or swift"
    );

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_csv_uses_scalar_type_boost() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"
interface Product {
  price: number;
}
"#;
    let request = json!({
        "jsonrpc": "2.0",
        "id": 151,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "typescript",
                "input_text": "price_text,price_value\nabc,12.5\n",
                "format": "csv"
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");
    let price_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "price")
        .expect("price mapping");
    assert_eq!(price_mapping.source.as_deref(), Some("price_value"));

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_single_line_interface() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "export interface Record { id: string; name?: string; }";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 16,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "typescript",
                "input_json": {
                    "id": 1,
                    "name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_single_line_rust_struct() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "pub struct Record { pub id: String, pub name: Option<String>, pub price: f64 }";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 19,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "rust",
                "input_json": {
                    "id": "001",
                    "name": "Ada",
                    "price": 100.0
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));
    assert_eq!(rule.mappings[2].source.as_deref(), Some("price"));

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_python_single_line_alias() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "class Record(BaseModel): id: str; name: Optional[str] = None; price: float = Field(alias=\"price_cents\")";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 20,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "python",
                "input_json": {
                    "id": "001",
                    "name": "Ada",
                    "price_cents": 100.0
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");

    let id_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "id")
        .expect("id mapping");
    assert_eq!(id_mapping.source.as_deref(), Some("id"));
    assert!(id_mapping.required);

    let name_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "name")
        .expect("name mapping");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    let price_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "price_cents")
        .expect("price mapping");
    assert_eq!(price_mapping.source.as_deref(), Some("price_cents"));
    assert!(price_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_go_single_line_tags() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "type Record struct { ID string `json:\"id\"` Name *string `json:\"name,omitempty\"` Price float64 `json:\"price\"` }";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 21,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "go",
                "input_json": {
                    "id": "001",
                    "name": "Ada",
                    "price": 100.0
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");

    let id_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "id")
        .expect("id mapping");
    assert_eq!(id_mapping.source.as_deref(), Some("id"));
    assert!(id_mapping.required);

    let name_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "name")
        .expect("name mapping");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_java_single_line_annotations() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "public class Record { @JsonProperty(\"user_id\") private String id; @SerializedName(\"full_name\") private Optional<String> name; }";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 22,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "java",
                "input_json": {
                    "user_id": "001",
                    "full_name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");

    let id_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "user_id")
        .expect("id mapping");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert!(id_mapping.required);

    let name_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "full_name")
        .expect("name mapping");
    assert_eq!(name_mapping.source.as_deref(), Some("full_name"));
    assert!(!name_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_kotlin_single_line_annotations() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "data class Record(@SerialName(\"user_id\") val id: String, @Json(name = \"full_name\") val name: String?, val price: Double)";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 23,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "kotlin",
                "input_json": {
                    "user_id": "001",
                    "full_name": "Ada",
                    "price": 100.0
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");

    let id_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "user_id")
        .expect("id mapping");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert!(id_mapping.required);

    let name_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "full_name")
        .expect("name mapping");
    assert_eq!(name_mapping.source.as_deref(), Some("full_name"));
    assert!(!name_mapping.required);

    let price_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "price")
        .expect("price mapping");
    assert_eq!(price_mapping.source.as_deref(), Some("price"));
    assert!(price_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_swift_single_line_coding_keys() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "struct Record: Codable { let id: String; let name: String?; let price: Double; enum CodingKeys: String, CodingKey { case id = \"user_id\", name, price = \"price_cents\" } }";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 24,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "swift",
                "input_json": {
                    "user_id": "001",
                    "name": "Ada",
                    "price_cents": 100.0
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");

    let id_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "user_id")
        .expect("id mapping");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert!(id_mapping.required);

    let name_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "name")
        .expect("name mapping");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    let price_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "price_cents")
        .expect("price mapping");
    assert_eq!(price_mapping.source.as_deref(), Some("price_cents"));
    assert!(price_mapping.required);

    server.shutdown();
}

#[test]
fn resources_list_and_read() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let list_request = json!({
        "jsonrpc": "2.0",
        "id": 17,
        "method": "resources/list"
    });
    let list_response = server.send(&list_request);
    let resources = list_response["result"]["resources"]
        .as_array()
        .expect("resources array");
    assert!(
        resources
            .iter()
            .any(|item| item["uri"] == "rulemorph://docs/rules_spec_en")
    );

    let read_request = json!({
        "jsonrpc": "2.0",
        "id": 18,
        "method": "resources/read",
        "params": {
            "uri": "rulemorph://docs/rules_spec_en"
        }
    });
    let read_response = server.send(&read_request);
    let text = read_response["result"]["contents"][0]["text"]
        .as_str()
        .expect("resource text");
    assert!(text.contains("Expr"));

    server.shutdown();
}

#[test]
fn prompts_list_and_get() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let list_request = json!({
        "jsonrpc": "2.0",
        "id": 18,
        "method": "prompts/list"
    });
    let list_response = server.send(&list_request);
    let prompts = list_response["result"]["prompts"]
        .as_array()
        .expect("prompts array");
    assert!(
        prompts
            .iter()
            .any(|item| item["name"] == "rule_from_input_base")
    );

    let get_request = json!({
        "jsonrpc": "2.0",
        "id": 19,
        "method": "prompts/get",
        "params": {
            "name": "explain_errors",
            "arguments": {
                "errors_json": "[{\"message\":\"oops\"}]"
            }
        }
    });
    let get_response = server.send(&get_request);
    let content = get_response["result"]["messages"][0]["content"]
        .as_str()
        .expect("prompt content");
    assert!(content.contains("Errors:"));

    server.shutdown();
}
