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
