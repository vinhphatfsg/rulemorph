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
