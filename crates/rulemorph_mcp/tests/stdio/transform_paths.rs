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
