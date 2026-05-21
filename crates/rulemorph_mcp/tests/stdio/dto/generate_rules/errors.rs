#[test]
fn generate_rules_from_dto_invalid_language_returns_json_rpc_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let response = call_tool(
        &mut server,
        1510,
        "generate_rules_from_dto",
        json!({
            "dto_text": "type Record = { id: string }",
            "dto_language": "ruby",
            "input_json": {
                "id": 1
            }
        }),
    );
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(
        response["error"]["message"],
        "dto_language must be rust, typescript, python, go, java, kotlin, or swift"
    );

    server.shutdown();
}
