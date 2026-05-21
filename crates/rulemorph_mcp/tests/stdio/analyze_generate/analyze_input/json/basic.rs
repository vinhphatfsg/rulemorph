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
