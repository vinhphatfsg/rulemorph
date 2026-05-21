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
