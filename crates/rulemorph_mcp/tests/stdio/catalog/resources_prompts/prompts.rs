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
