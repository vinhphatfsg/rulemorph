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
fn analyze_input_max_paths_limits_reported_paths() {
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
                    "id": 1,
                    "name": "Ada",
                    "active": true
                },
                "max_paths": 2
            }
        }
    });

    let response = server.send(&request);
    let paths = response["result"]["meta"]["paths"]
        .as_array()
        .expect("paths array");
    assert_eq!(paths.len(), 2);
    assert_eq!(response["result"]["meta"]["summary"]["paths"], json!(2));

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
