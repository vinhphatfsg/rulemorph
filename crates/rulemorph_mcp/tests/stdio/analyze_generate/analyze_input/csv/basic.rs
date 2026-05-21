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
